from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import json

class RestServer(object):
    class S(BaseHTTPRequestHandler):
        def _set_headers(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()

        def do_GET(self):
            self._set_headers()
            self.wfile.write("<html><body><h1>hi!</h1></body></html>")

        def do_HEAD(self):
            self._set_headers()
            
        def do_POST(self):
            # Doesn't do anything with posted data
            content_length = int(self.headers['Content-Length']) # <--- Gets the size of data
            post_data = self.rfile.read(content_length) # <--- Gets the data itself
            self._set_headers()
            json_obj = json.loads(post_data)        
            self.wfile.write("<html><body><h1>POST!</h1><pre>" + str(json_obj) + "</pre></body></html>")

    def __init__(self, port = 80):
        self._server_address = ('', port)
        self._server_class = HTTPServer
        self._handler_class = self.S
        self._port = port
        self._httpd = self._server_class(self._server_address, self._handler_class)

    def run(self):        
        print 'Starting httpd...'
        self._httpd.serve_forever()            
    
    def stop(self):
        self._httpd.shutdown()   

# if __name__ == "__main__":
#     from sys import argv
# 
#     if len(argv) == 2:
#         run(port=int(argv[1]))
#     else:
#         run()