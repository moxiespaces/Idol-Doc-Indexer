var fs = require('fs');
var stomp = require('stomp');
var graylog = require('graylog');

console.log("Starting stomp_client.js");

// read preferences json file from disk
var pref_file = fs.readFileSync(__dirname + '/stomp_preferences.json', 'utf8');
var pref = JSON.parse(pref_file);

GLOBAL.graylogHost = pref.graylogHost;
GLOBAL.graylogFacility = pref.graylogFacility;
GLOBAL.graylogPort = pref.graylogPort;
GLOBAL.graylogToConsole = pref.graylogToConsole;

StompLogging.prototype.debug = function(message) {
    log("debug: " + message, {level: LOG_DEBUG});
};

StompLogging.prototype.warn = function(message) {
    log("warn: " + message, {level: LOG_WARNING});
};

StompLogging.prototype.error = function(message, die) {
    log("error: " + message, {level: LOG_ERROR});
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

var client = new stomp.Stomp(stomp_args);

var headers = {
    destination: pref.queue,
    ack: "client-individual",
    "activemq.prefetchSize": 10,
    "activemq.priority": 50
};


client.on('connected', function() {
    client.subscribe(headers);
    console.log("Connected");
});


client.on('message', function(message) {
    var message_id = message.headers['message-id'];
    var txn_id = message.headers['txn_id'] || '1';

    // the body is json
    var body = message.body;
    log("message_id: " + message_id + ", body: " + body, {"_txn_id": txn_id});

    var json_data = JSON.parse(body);

    // the action will be either index or unindex
    var action = json_data.action;

    function ack() {
        client.ack(message.headers['message-id']);
    }

    function nack() {
        // not yet supported
    }

    if ( json_data.action == "unindex" ) {
        // ***** UNINDEX ****** //
        autonomy.unindex(json_data, ack, nack, txn_id);
    } else if ( json_data.action == "index" ) {
        // ****** INDEX ****** //
        autonomy.index(json_data, ack, nack, txn_id);
    } else {
        // ****** unknown message. lets just delete it so we don't keep getting it ****** //
        ack();
    }
});

client.on('error', function(error_frame) {
    log(error_frame.body, {level: LOG_ERROR});
});

process.on('SIGINT', function() {
    log("SIGINT received: disconnecting...", {level: LOG_ERROR});
    client.disconnect();
});

client.connect();

