var http = require('http');
var fs = require('fs');
var knox = require('knox');
var path = require('path');
var querystring = require('querystring');

function Autonomy(docs_dir) {
    this.docs_dir = docs_dir;
}


Autonomy.prototype.index = function (json_data, ctx) {
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

    ctx.log("submitting the s3 request to download the document");
    var s3_path = file_id + '/' + querystring.escape(file_name);

    // request the s3 document
    client.get(s3_path).on('response', function (s3_res) {
        ctx.log("downloading " + s3_path + " from s3...");
        ctx.log(s3_res.statusCode);
        ctx.log(s3_res.headers);

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
            ctx.log("Submitting the data to autonomy filesystemfetch");
            outstream.end();
            var xml = "<?xml version=\"1.0\"?><autn:import><autn:envelope><autn:stubidx><![CDATA[" + stubidx + "]]></autn:stubidx><autn:document><autn:fetch url=\"" + path_to_file + "\" deleteoriginal=\"true\" /></autn:document></autn:envelope></autn:import>";
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
            var post_req = http.request(http_options, function (res) {
                res.setEncoding('utf8');
                res.on('data', function (chunk) {
                    ctx.log('Response: ' + chunk);
                    ctx.ack();
                });
                res.on('error', function (e) {
                    ctx.log('problem with request: ' + e.message);
                    ctx.nack();
                });
            });

            post_req.on('error', function(error) {
                ctx.log('error trying to make autonomy request ' + error);
                ctx.nack();
            });


            // post the data
            post_req.write(idol_data);
            post_req.end();
        });
    }).end();
}

Autonomy.prototype.unindex = function(json_data, ctx) {

    // unique autonomy id
    var reference = json_data.reference;

    // each customer has their own autonomy db
    var db = json_data.db;
    ctx.log("unindex reference: " + reference + ", db: " + db);

    var http_options = {
        host: 'localhost',
        port: 9001,
        path: '/DREDELETEREF?Docs=' + reference + '&DREDbName=' + db
    };

    // post to autonomy to unindex the document
    var unindex_request = http.get(http_options, function (res) {
        ctx.log("unindex response: " + res.statusCode);
        ctx.ack()
    }).on('error', function (e) {
        ctx.log("unindex error: " + e.message);
        ctx.nack();
    });
}


module.exports = Autonomy;
