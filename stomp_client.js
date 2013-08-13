var fs = require('fs');
var stomp = require('stomp');
var os = require('os');
var dateFormat = require('dateformat');

var http = require('http');
var knox = require('knox');
var path = require('path');
var querystring = require('querystring');

var process_state = "STARTING";

var consoleOnly = false;
var vargs = process.argv.slice(2);

for (var idx=0; idx<vargs.length; idx++) {
    if (vargs[idx] === "--console") {
        consoleOnly = true;
    }
}

// read preferences json file from disk
var pref_file = fs.readFileSync(__dirname + '/stomp_preferences.json', 'utf8');
var pref = JSON.parse(pref_file);


var winston = require('winston');

var logger = new (winston.Logger)({
    transports: [ ]
});

if (consoleOnly) {
    logger.add(winston.transports.Console, {timestamp: true});
} else {
    logger.add(winston.transports.File, {
        filename: pref.logfile || 'idol.log',
        timestamp: true,
        maxFiles: pref.logfileMaxFiles || 5,
        maxsize: pref.logfileMaxSize || 10485760 // 10 MB
    });
}

if (pref.graylogEnabled) {
    var graylogOpts = {
        graylogHost: pref.graylogHost,
        graylogPort: pref.graylogPort,
        graylogFacility: pref.graylogFacility
    };

    var Graylog2 = require('winston-graylog2').Graylog2;
    logger.add(Graylog2, graylogOpts);
}


var stomp_args = {
    port: pref.port,
    host: pref.host,
    debug: pref.debug,
    login: pref.login,
    passcode: pref.passcode
};

var client = new stomp.Stomp(stomp_args);

client.log.debug = function(message) { logger.debug(message); };
client.log.warn = function(message) { logger.warn(message); };
client.log.error = function(message, die) {
    logger.error(message);
    if (die) {
        process.exit(1);
    }
};


var headers = {
    id: 1,
    destination: pref.queue,
    ack: "client-individual",
    "activemq.prefetchSize": 100,
    "activemq.priority": 50
};

/**
 * Utility function to add a single leading zero, if necessary.
 *
 * @param n number between 0 and 99
 * @return {String}
 */
function pad2(n) { return n<10 ? '0'+n : '' + n; }

// set default podName and then determine actual podName, based on queue being listened to.
// this name will be used in job stats messages.
var podName = "autonomy";
if ((/^\/queue\/worq\..+/).test(pref.queue)) {
    podName = pref.queue.substring("/queue/worq.".length);
}
logger.info("starting stomp_client.js for '" + podName + "'");

var messageId = 1;

client.on('connected', function() {
    client.subscribe(headers);
    logger.info("Connected");
});

/**
 * This function is used to format a javascript Date object in the format
 * required by the stats engine.  This function uses the dateFormat module.
 *
 * @param d {Date}
 */
function dateToString(d) {
    return dateFormat(d, "yyyy-mm-dd hh:MM:ss.l o");
}

/**
 * This function is used by an Array.map method as a callback to ensure that
 * all elements have their leading and tailing whitespace remove.
 *
 * @param str
 * @return {*}
 */
function map_trim(str) {
    return str.trim();
}

/**
 * This function is used by an Array.filter method as a callback to filter
 * out empty elements.
 *
 * @param str {String}
 * @return {Boolean}
 */
function filter_empty(str) {
    if (str === undefined) {
        return false;
    } else {
        return (str.length > 0);
    }
}

var messages = [];

function MessageContext(message) {
    messages.push(this);

    this.message = message;
    this.startTime = new Date();
    this.txn_id = message.headers['txn_id'] || this.startTime.getTime();
    this.message_id = message.headers['message-id'];
    this.job_enqueue_timestamp = new Date(parseInt(message.headers['timestamp'], 10));
    this.body = message.body;
    this.json_data = JSON.parse(this.body);
    this.action = this.json_data.action;
    this.attempts = message.headers['attempts'] || 0;
}

MessageContext.prototype.log = function(msg) {
    logger.debug(msg, {"_transaction_id": this.txn_id});
};

MessageContext.prototype.send_stats = function(endTime, elapsed, job_status) {
    var stats_msg = JSON.stringify({
        job_id: "",
        txn_id: this.txn_id,
        site_id: this.message.headers["site_id"],
        pod: podName,
        host: os.hostname,
        process: process.pid,
        label: this.action,
        status: job_status,
        job_enqueue_time: dateToString(this.job_enqueue_timestamp),
        start_time: dateToString(this.startTime),
        end_time: dateToString(endTime),
        elapsed: elapsed
    });

    this.log("publishing stats: " + stats_msg);
    var send_headers = {
        destination: '/topic/jobstats',
        body: stats_msg,
        "amq-msg-type": "text"
    };
    client.send(send_headers, false);
};

/**
 * This function will process an exception by adding the exception information to the
 * body object (json_data) and sending it to the failed queue.  This will allow mothership
 * to display the error.
 *
 * @param ex {Error}
 */
MessageContext.prototype.process_exception = function(ex) {
    var trace = ex.stack.split("\n").map(map_trim).filter(filter_empty);
    trace.shift(); // remove first element, which happens to be the error message again.

    this.json_data["error"] = {
        ":err": {class_name: "Error", message: ex.message},
        ":trace": trace
    };

    var send_headers = {
        destination: "/queue/worq." + podName + "_failed",
        body: JSON.stringify(this.json_data),
        pod: this.message.headers["pod"],
        site_id: this.message.headers["site_id"],
        attempts: this.attempts + 1,
        txn_id: this.txn_id,
        "amq-msg-type": "text"
    };

    client.send(send_headers, false);
    this.removeMessage();
    this.log("Exception: " + ex + "\n   " + trace.join("\n   "));
    this.nack();
};

MessageContext.prototype.ack = function() {
    var endTime = new Date();
    var elapsed = (endTime.getTime() - this.startTime.getTime()) / 1000;
    client.ack(this.message_id);
    this.send_stats(endTime, elapsed, "SUCCESS");
    this.log("ACK: " + this.message_id + " elapsed: " + elapsed + " seconds");
    this.cleanup();
};

MessageContext.prototype.nack = function() {
    var endTime = new Date();
    var elapsed = (endTime.getTime() - this.startTime.getTime()) / 1000;
    client.ack(this.message_id);
    this.send_stats(endTime, elapsed, "FAILED");
    this.log("NACK: " + this.message_id + " elapsed: " + elapsed + " seconds");
    this.cleanup();
};

MessageContext.prototype.removeMessage = function() {
    var idx = messages.indexOf(this);
    if (idx >= 0) {
        messages.splice(idx, 1);
    }
};

MessageContext.prototype.is_shutting_down = function() {
    return (process_state === "RUNNING") ? false : true;
};

MessageContext.prototype.cleanup = function() {
    var ctx = this;
    if (ctx.path_to_file) {
        fs.stat(ctx.path_to_file, function(err, stats) {
            if (err) { return; }
            if (stats.isFile()) {
                fs.unlink(ctx.path_to_file);
            }
        });
    }
    this.removeMessage();
    return true;
};

MessageContext.prototype.index = function () {
    var ctx = this;

    var key = ctx.json_data.s3_aws_key;
    var secret = ctx.json_data.s3_aws_secret;
    var bucket = ctx.json_data.s3_bucket;
    var file_id = ctx.json_data.file_id;
    var file_name = ctx.json_data.file_name;
    var autonomy_db_name = ctx.json_data.autonomy_db_name;
    var stubidx = ctx.json_data.stubidx;

    var docs_dir = pref.docs_dir;

    // create the s3 client
    var knoxClient = knox.createClient({
        key: key,
        secret: secret,
        bucket: bucket
    });

    ctx.log("submitting the s3 request to download the document");
    var s3_path = file_id + '/' + querystring.escape(file_name);

    // request the s3 document
    ctx.s3_request = knoxClient.get(s3_path);
    ctx.s3_request.on('error', function(err) {
        if (ctx.is_shutting_down()) { return ctx.cleanup(); }
        ctx.log("error on s3 request: " + err);
        ctx.nack();
    });
    ctx.s3_request.on('response', function (s3_res) {
        if (ctx.is_shutting_down()) { return ctx.cleanup(); }
        ctx.s3_request = null;

        ctx.log("downloading " + s3_path + " from s3...");
        ctx.log("download status code: " + s3_res.statusCode);

        // create a directory with the same name as the db on the autonomy server
        // var unique_name = (autonomy_db_name + "/" + file_id + "/" + file_name).replace(/\//g, "_");
        var now = new Date();

        var unique_name = [
            now.getFullYear(), pad2(now.getMonth()+1), pad2(now.getDate()),
            '-', pad2(now.getHours()), pad2(now.getMinutes()), pad2(now.getSeconds()),
            '-', process.pid,
            '-', (Math.random() * 0x100000000 + 1).toString(36)
        ].join('');
        ctx.path_to_file = docs_dir + unique_name;

        // stream the document to disk chunk by chunk
        var outstream = fs.createWriteStream(ctx.path_to_file);
        s3_res.on('data', function (chunk) {
            outstream.write(chunk);
        });

        // the file has been saved! now let's build the autonomy request
        s3_res.on('end', function () {
            if (ctx.is_shutting_down()) { return ctx.cleanup(); }
            ctx.log("Submitting the data to autonomy filesystemfetch");
            outstream.end();
            var xml = "<?xml version=\"1.0\"?><autn:import><autn:envelope><autn:stubidx><![CDATA[" + stubidx + "]]></autn:stubidx><autn:document><autn:fetch url=\"" + ctx.path_to_file + "\" deleteoriginal=\"true\" /></autn:document></autn:envelope></autn:import>";
            ctx.log(xml);

            var idol_data = querystring.stringify({
                'Data': stubidx,
                'DREDBNAME': autonomy_db_name,
                'EnvelopeXML': xml,
                'jobname': 'ImportEnvelopeJob',
                'EnvelopeImportFailOnImport': 'never'
            });

            var http_options = {
                host: 'localhost',
                port: 7000,
                path: '/action=ImportEnvelope',
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Content-Length': idol_data.length
                }
            };

            // post to autonomy to index the document
            ctx.index_req = http.request(http_options, function (res) {
                res.setEncoding('utf8');
                var autonomy_response = "";
                res.on('data', function (chunk) {
                    if (ctx.is_shutting_down()) {
                        return;
                    }
                    autonomy_response = autonomy_response + chunk;
                });
                res.on('end', function() {
                    if (ctx.is_shutting_down()) { return ctx.cleanup(); }
                    ctx.log('Autonomy Response: ' + autonomy_response);
                    ctx.ack();
                });
                res.on('error', function (e) {
                    ctx.log('Autonomy problem with request: ' + e.message);
                    ctx.nack();
                });
            });

            ctx.index_req.on('error', function(error) {
                if (ctx.is_shutting_down()) { return ctx.cleanup(); }
                ctx.log('error trying to make autonomy request ' + error);
                ctx.nack();
            });

            // post the data
            ctx.index_req.write(idol_data);
            ctx.index_req.end();
        });
    }).end();
};

MessageContext.prototype.unindex = function() {
    var ctx = this;

    // unique autonomy id
    var reference = ctx.json_data.reference;

    // each customer has their own autonomy db
    var db = ctx.json_data.db;
    ctx.log("unindex reference: " + reference + ", db: " + db);

    var http_options = {
        host: 'localhost',
        port: 9001,
        path: '/DREDELETEREF?Docs=' + reference + '&DREDbName=' + db
    };

    // post to autonomy to unindex the document
    ctx.unindex_req = http.get(http_options, function (res) {
        if (ctx.is_shutting_down()) { return ctx.cleanup(); }
        ctx.log("unindex response: " + res.statusCode);
        ctx.ack();
    }).on('error', function (e) {
        if (ctx.is_shutting_down()) { return ctx.cleanup(); }
        ctx.log("unindex error: " + e.message);
        ctx.nack();
    });
};

client.on('message', function(message) {
    if (process_state !== "RUNNING") { return; }

    var ctx = new MessageContext(message);
    ctx.log("message_id: " + ctx.message_id + " body: " + ctx.body);

    if (ctx.action === "unindex") {
        try {
            ctx.unindex();
        } catch (ex) {
            ctx.process_exception(ex);
        }
    } else if (ctx.action === "index") {
        try {
            ctx.index();
        } catch (ex) {
            ctx.process_exception(ex);
        }
    } else {
        // ****** unknown message. lets just delete it so we don't keep getting it ****** //
        ctx.ack();
    }
});

client.on('error', function(error_frame) {
    var msg = "stomp_client error => ";
    if (error_frame) {
        if (error_frame.body) {
            msg = msg + error_frame.body;
        } else {
            msg = msg + error_frame.toString();
        }
    } else {
        msg = msg + " Unknown Error";
    }
    logger.error(msg);
});

function waitForComplete() {
    if (messages.length === 0) {
        logger.info("No messages outstanding...disconnecting");
        client.disconnect();
    } else {
        logger.info("Messages outstanding: " + messages.length);
        setTimeout(waitForComplete, 1000);
    }
}

process.on('SIGINT', function() {
    if (process_state === "RUNNING") {
        process_state = "SHUTTING DOWN";
        logger.info("SIGINT received: shutting down...");
        client.unsubscribe({id: 1, destination: pref.queue});
        var s3_aborts = 0;
        var index_aborts = 0;
        var unindex_aborts = 0;
        for (var i=0; i<messages.length; i++) {
            var ctx = messages[i];
            if (ctx.s3_request) {
                s3_aborts++;
                ctx.s3_request.abort();
                ctx.s3_request = null;
            } else if (ctx.index_req) {
                index_aborts++;
                ctx.index_req.abort();
                ctx.index_req = null;
            } else if (ctx.unindex_req) {
                unindex_aborts++;
                ctx.unindex_req.abort();
                ctx.unindex_req = null;
            }
        }
        if ((s3_aborts + index_aborts) > 0) {
            logger.info("Aborted " + s3_aborts + " s3, " + index_aborts + " index, and " + unindex_aborts + " unindex requests.");
        }
        waitForComplete();
    } else if (process_state === "STARTING") {
    } else {
        logger.info("SIGINT received a second time: aborting...");
        client.disconnect();
        setTimeout(terminate, 2000); // give it up to 2 seconds.

    }
});

function terminate() { process.exit(1); }

client.on('disconnected', function() {
    var msg = "finished with " + messages.length + " messages outstanding.";
    logger.info(msg, terminate);
});

client.on('connected', function() {
    process_state = "RUNNING";
});

client.connect();

