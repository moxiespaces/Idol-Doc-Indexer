var http = require('http');
var fs = require('fs');
var knox = require('knox');
var path = require('path');
var querystring = require('querystring');

function Autonomy(docs_dir) {
    this.docs_dir = docs_dir;
}


Autonomy.prototype.index = function (json_data, ack, nack, txn_id) {
    var key = json_data.s3_aws_key;
    var secret = json_data.s3_aws_secret;
    var bucket = json_data.s3_bucket;
    var file_id = json_data.file_id;
    var file_name = json_data.file_name;
    var autonomy_db_name = json_data.autonomy_db_name;
    var stubidx = json_data.stubidx;

    var docs_dir = this.docs_dir;

    // create the s3 client
    var client = knox.createClient({
        key: key,
        secret: secret,
        bucket: bucket
    });

    log("submitting the s3 request to download the document", {"_txn_id": txn_id});
    var s3_path = file_id + '/' + querystring.escape(file_name);

    // request the s3 document
    client.get(s3_path).on('response', function (s3_res) {
        log("downloading " + s3_path + " from s3...", {"_txn_id": txn_id});
        log(s3_res.statusCode, {"_txn_id": txn_id});
        log(s3_res.headers, {"_txn_id": txn_id});

        // create a directory with the same name as the db on the autonomy server
        var unique_name = (autonomy_db_name + "/" + file_id + "/" + file_name).replace(/\//g, "_")
        var path_to_file = docs_dir + unique_name;

        // stream the document to disk chunk by chunk
        var outstream = fs.createWriteStream(path_to_file);
        s3_res.on('data', function (chunk) {
            outstream.write(chunk);
        });

        // the file has been saved! now let's build the autonomy request
        s3_res.on('end', function () {
            log("Submitting the data to autonomy filesystemfetch", {"_txn_id": txn_id});
            outstream.end();
            var xml = "<?xml version=\"1.0\"?><autn:import><autn:envelope><autn:stubidx><![CDATA[" + stubidx + "]]></autn:stubidx><autn:document><autn:fetch url=\"" + path_to_file + "\" deleteoriginal=\"true\" /></autn:document></autn:envelope></autn:import>";
            log(xml, {"_txn_id": txn_id});

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
            var post_req = http.request(http_options, function (res) {
                res.setEncoding('utf8');
                res.on('data', function (chunk) {
                    console.log('Response: ' + chunk);
                    ack();
                });
                res.on('error', function (e) {
                    console.log('problem with request: ' + e.message);
                    nack();
                });
            });

            post_req.on('error', function(error) {
                log('error trying to make autonomy request ' + error, {"_txn_id": txn_id});
                nack();
            });


            // post the data
            post_req.write(idol_data);
            post_req.end();
        });
    }).end();
}

Autonomy.prototype.unindex = function(json_data, ack, nack, txn_id) {

    // unique autonomy id
    var reference = json_data.reference;

    // each customer has their own autonomy db
    var db = json_data.db;
    log("unindex reference: " + reference + ", db: " + db, {"_txn_id": txn_id});

    var http_options = {
        host: 'localhost',
        port: 9001,
        path: '/DREDELETEREF?Docs=' + reference + '&DREDbName=' + db
    };

    // post to autonomy to unindex the document
    var unindex_request = http.get(http_options, function (res) {
        log("unindex response: " + res.statusCode, {"_txn_id": txn_id});
        ack()
    }).on('error', function (e) {
        log("unindex error: " + e.message, {"_txn_id": txn_id});
        nack();
    });
}


module.exports = Autonomy;
