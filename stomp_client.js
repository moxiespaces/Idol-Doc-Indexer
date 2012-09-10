var fs = require('fs');
var stomp = require('stomp');
var os = require('os');
var dateFormat = require('dateformat');

// read preferences json file from disk
var pref_file = fs.readFileSync(__dirname + '/stomp_preferences.json', 'utf8');
var pref = JSON.parse(pref_file);


var winston = require('winston');

var logger = new (winston.Logger)({
    transports: [ ]
});

logger.add(winston.transports.Console, {timestamp: true});

var graylogOpts = {
    graylogHost: pref.graylogHost,
    graylogPort: pref.graylogPort,
    graylogFacility: pref.graylogFacility
};

var Graylog2 = require('winston-graylog2').Graylog2;
logger.add(Graylog2, graylogOpts);


StompLogging.prototype.debug = function(message) {
    logger.debug(message);
};

StompLogging.prototype.warn = function(message) {
    logger.warn(message);
};

StompLogging.prototype.error = function(message, die) {
    logger.error(message);
    if (die) {
        process.exit(1);
    }
};

var Autonomy = require('./autonomy');
var autonomy = new Autonomy(pref.docs_dir);


var stomp_args = {
    port: pref.port,
    host: pref.host,
    debug: pref.debug,
    login: pref.login,
    passcode: pref.passcode
};

var client = new stomp.Stomp(stomp_args);

var headers = {
    id: 1,
    destination: pref.queue,
    ack: "client-individual",
    "activemq.prefetchSize": 100,
    "activemq.priority": 50
};

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

var messages_outstanding_count = 0;
var msg_counter = 1;
var messages = {};

function MessageContext(message) {
    this.msg_number = msg_counter++;
    messages_outstanding_count++;
    messages[this.msg_number] = this;

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
    this.removeMessage();
    this.send_stats(endTime, elapsed, "SUCCESS");
    this.log("ACK: " + this.message_id + " elapsed: " + elapsed + " seconds");
};

MessageContext.prototype.nack = function() {
    var endTime = new Date();
    var elapsed = (endTime.getTime() - this.startTime.getTime()) / 1000;
    client.ack(this.message_id);
    this.removeMessage();
    this.send_stats(endTime, elapsed, "FAILED");
    this.log("NACK: " + this.message_id + " elapsed: " + elapsed + " seconds");
};

MessageContext.prototype.removeMessage = function() {
    delete messages[this.msg_number];
    messages_outstanding_count--;
};


client.on('message', function(message) {
    var ctx = new MessageContext(message);
    ctx.log("message_id: " + ctx.message_id + " body: " + ctx.body);

    if (ctx.action === "unindex") {
        try {
            autonomy.unindex(ctx.json_data, ctx);
        } catch (ex) {
            ctx.process_exception(ex);
        }
    } else if (ctx.action === "index") {
        try {
            autonomy.index(ctx.json_data, ctx);
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

process.on('SIGINT', function() {
    logger.info("SIGINT received: disconnecting...");
    client.unsubscribe({id: 1, destination: pref.queue});
    client.disconnect();
});

client.on('disconnected', function(err) {
    var msg = "finished with " + messages_outstanding_count + " messages outstanding.";
    logger.info(msg, function() {
        process.exit(1);
    });
});

client.connect();

