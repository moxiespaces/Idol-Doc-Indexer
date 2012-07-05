var fs = require('fs');
var stomp = require('stomp');
var graylog = require('graylog');
var os = require('os');
var dateFormat = require('dateformat');

// read preferences json file from disk
var pref_file = fs.readFileSync(__dirname + '/stomp_preferences.json', 'utf8');
var pref = JSON.parse(pref_file);

GLOBAL.graylogHost = pref.graylogHost;
GLOBAL.graylogFacility = pref.graylogFacility;
GLOBAL.graylogPort = pref.graylogPort;
GLOBAL.graylogToConsole = pref.graylogToConsole;

var whatami = "nodejs";

StompLogging.prototype.debug = function(message) {
    log("debug: " + message, {facility: whatami, level: LOG_DEBUG});
};

StompLogging.prototype.warn = function(message) {
    log("warn: " + message, {facility: whatami, level: LOG_WARNING});
};

StompLogging.prototype.error = function(message, die) {
    log("error: " + message, {facility: whatami, level: LOG_ERROR});
    if (die)
        process.exit(1);
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

log("Starting stomp_client.js");

var client = new stomp.Stomp(stomp_args);

var headers = {
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
    log("PodName=" + podName);
}


client.on('connected', function() {
    client.subscribe(headers);
    log("Connected", {facility: whatami});
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
    if (str === undefined) return false;
    return (str.length > 0);
}


client.on('message', function(message) {
    var startTime = new Date();
    var message_id = message.headers['message-id'];
    var txn_id = message.headers['txn_id'] || startTime.getTime();
    var job_enqueue_timestamp = new Date(new Number(message.headers['timestamp']));

    // the body is json
    var body = message.body;
    log("message_id: " + message_id + " body: " + body, {facility: whatami, level: LOG_DEBUG, "_transaction_id": txn_id});

    var json_data = JSON.parse(body);

    // the action will be either index or unindex
    var action = json_data.action;

    function graylog(msg) {
        GLOBAL.log(msg, {facility: whatami, level: LOG_DEBUG, "_transaction_id": txn_id});
    }

    function send_stats(endTime, elapsed, job_status) {
        var stats_msg = JSON.stringify({
            job_id: "",
            txn_id: txn_id,
            site_id: message.headers["site_id"],
            pod: podName,
            host: os.hostname,
            process: process.pid,
            label: json_data.action,
            status: job_status,
            job_enqueue_time: dateToString(job_enqueue_timestamp),
            start_time: dateToString(startTime),
            end_time: dateToString(endTime),
            elapsed: elapsed
        });

        graylog("publishing stats: " + stats_msg);
        var send_headers = {
            destination: '/topic/jobstats',
            body: stats_msg,
            "amq-msg-type": "text"
        };
        client.send(send_headers, false);
    }

    function ack() {
        endTime = new Date();
        elapsed = (endTime.getTime() - startTime.getTime()) / 1000;
        client.ack(message.headers['message-id']);
        send_stats(endTime, elapsed, "SUCCESS");
        graylog("ACK: " + message_id + " elapsed: " + elapsed + " seconds");
    }

    function nack() {
        endTime = new Date();
        elapsed = (endTime.getTime() - startTime.getTime()) / 1000;
        client.ack(message.headers['message-id']);
        send_stats(endTime, elapsed, "FAILED");
        graylog("NACK: " + message_id + " elapsed: " + elapsed + " seconds");
    }

    /**
     * This function will process an exception by adding the exception information to the
     * body object (json_data) and sending it to the failed queue.  This will allow mothership
     * to display the error.
     *
     * @param ex {Error}
     */
    function process_exception(ex) {
        var trace = ex.stack.split("\n").map(map_trim).filter(filter_empty);
        trace.shift(); // remove first element, which happens to be the error message again.

        json_data["error"] = {
            ":err": {class_name: "Error", message: ex.message},
            ":trace": trace
        };

        var send_headers = {
            destination: "/queue/worq." + podName + "_failed",
            body: JSON.stringify(json_data),
            pod: message.headers["pod"],
            site_id: message.headers["site_id"],
            txn_id: txn_id,
            "amq-msg-type": "text"
        };
        client.send(send_headers, false);
        graylog("Exception: " + ex + "\n   " + trace.join("\n   "));
        nack();
    }

    var ctx = {
        log: graylog,
        ack: ack,
        nack: nack
    };

    if ( json_data.action == "unindex" ) {
        // ***** UNINDEX ****** //
        try {
            autonomy.unindex(json_data, ctx);
        } catch (ex) {
            process_exception(ex);
        }
    } else if ( json_data.action == "index" ) {
        // ****** INDEX ****** //
        try {
            autonomy.index(json_data, ctx);
        } catch (ex) {
            process_exception(ex);
        }
    } else {
        // ****** unknown message. lets just delete it so we don't keep getting it ****** //
        ack();
    }
});

client.on('error', function(error_frame) {
    var msg = "ERROR: " + (error_frame.body) ? error_frame.body : error_frame.toString;
    log(msg, {facility: whatami, level: LOG_ERROR});
});

process.on('SIGINT', function() {
    log("SIGINT received: disconnecting...", {facility: whatami, level: LOG_ERROR});
    client.disconnect();
});

client.connect();

