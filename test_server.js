var http = require('http');
var querystring = require('querystring');
var DOMParser = require('xmldom').DOMParser;
var fs = require('fs');

http.createServer(function (req, res) {
    if (req.method === "POST") {
        var str = "";
        req.on('data', function(chunk) {
            str = str + chunk;
        });
        req.on('end', function() {
            var filePath = null;
            var deleteIt = false;

            var reqMsg = querystring.parse(str);
            if (reqMsg && reqMsg['EnvelopeXML']) {
                str = reqMsg['EnvelopeXML'];
                var doc = new DOMParser().parseFromString(str, 'text/xml');
                var elements = doc.getElementsByTagName("autn:fetch");
                if (elements.length === 1) {
                    var fetch = elements.item(0);
                    filePath = fetch.getAttribute("url");
                    var deleteOriginal = fetch.getAttribute("deleteoriginal");
                    if (deleteOriginal === "true") { deleteIt = true; }
                    str = "file=" + filePath + " deleteIt=" + deleteIt;
                } else {
                    str = "" + elements;
                }
                // str = "" + doc;
            }
            console.log("POST received: " + str);
            setTimeout(function() {
                if ((filePath !== null) && (deleteIt)) {
                    fs.unlink(filePath, function(err) {
                        if (err) { return; }
                        console.log("successfully deleted " + filePath);
                    });
                }
                res.writeHead(200, {'Content-Type': 'text/plain'});
                res.end("Hello World\n");
            }, 2000);
        });
    } else {
        console.log("GET request received");
        setTimeout(function() {
            res.writeHead(200, {'Content-Type': 'text/plain'});
            res.end('Hello World\n');
            console.log("Sent Response");
        }, 2000);
    }

}).listen(7000, '127.0.0.1');

console.log('Server running at http://127.0.0.1:7000/');