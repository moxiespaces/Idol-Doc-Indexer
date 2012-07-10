var http = require('http');
http.createServer(function (req, res) {
    console.log("Received Request");
    setTimeout(function() {
        res.writeHead(200, {'Content-Type': 'text/plain'});
        res.end('Hello World\n');
        console.log("Sent Response");
    }, 2000);
}).listen(7000, '127.0.0.1');

console.log('Server running at http://127.0.0.1:7000/');