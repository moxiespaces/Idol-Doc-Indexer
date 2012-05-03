var fs = require('fs');
var stomp = require('stomp');
var graylog = require('graylog');

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
    "activemq.prefetchSize": 10,
    "activemq.priority": 50
};


client.on('connected', function() {
    client.subscribe(headers);
    log("Connected", {facility: whatami});
});


client.on('message', function(message) {
    var startTime = new Date();
    var message_id = message.headers['message-id'];
    var txn_id = message.headers['txn_id'] || startTime.getTime();

    // the body is json
    var body = message.body;
    log("message_id: " + message_id + " body: " + body, {facility: whatami, level: LOG_DEBUG, "_transaction_id": txn_id});

    var json_data = JSON.parse(body);

    // the action will be either index or unindex
    var action = json_data.action;

    function graylog(msg) {
        GLOBAL.log(msg, {facility: whatami, level: LOG_DEBUG, "_transaction_id": txn_id});
    }

    function ack() {
        endTime = new Date();
        elapsed = (endTime.getTime() - startTime.getTime()) / 1000;
        graylog("ACK: " + message_id + " elapsed: " + elapsed + " seconds");
        client.ack(message.headers['message-id']);
    }

    function nack() {
        endTime = new Date();
        elapsed = (endTime.getTime() - startTime.getTime()) / 1000;
        graylog("NACK: " + message_id + " elapsed: " + elapsed + " seconds");
        client.ack(message.headers['message-id']);
        // not yet supported
    }

    var ctx = {
        log: graylog,
        ack: ack,
        nack: nack
    };

    if ( json_data.action == "unindex" ) {
        // ***** UNINDEX ****** //
        autonomy.unindex(json_data, ctx);
    } else if ( json_data.action == "index" ) {
        // ****** INDEX ****** //
        autonomy.index(json_data, ctx);
    } else {
        // ****** unknown message. lets just delete it so we don't keep getting it ****** //
        ack();
    }
});

client.on('error', function(error_frame) {
    log(error_frame.body, {facility: whatami, level: LOG_ERROR});
});

process.on('SIGINT', function() {
    log("SIGINT received: disconnecting...", {facility: whatami, level: LOG_ERROR});
    client.disconnect();
});

client.connect();

