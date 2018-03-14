import json
import logging
import threading
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

import logging_aux
from heartbeat_table import HeartBeatTable


class RestServer(object):  # TODO: replace this implementation with twisted based implementation
    def __init__(self, heartbeat_table, port=80):
        self.logger = logging_aux.init_logger_aux("hpcframework.heatbeat_server", "hpcframework.heatbeat_server.log")
        self._heartbeat_table = heartbeat_table  # type: HeartBeatTable
        self._server_address = ('', port)
        self._server_class = HTTPServer
        self._handler_class = HeartBeatHandler
        self._port = port
        self._httpd = self._server_class(self._server_address, self._handler_class)
        self._server_thread = threading.Thread(target=self.run)
        HeartBeatHandler.logger = self.logger
        HeartBeatHandler.heartbeat_table = self._heartbeat_table

    def run(self):
        self.logger.debug('Starting httpd...')
        self._httpd.serve_forever()

    def stop(self):
        self._httpd.shutdown()
        self._server_thread.join()

    def start(self):
        self._server_thread.start()


class HeartBeatHandler(BaseHTTPRequestHandler):
    logger = None  # type: logging.Logger
    heartbeat_table = None  # type: HeartBeatTable

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        self.wfile.write("<html><body><h1>hi from thread {}!</h1></body></html>".format(threading._get_ident()))

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        # Doesn't do anything with posted data
        content_length = int(self.headers['Content-Length'])  # <--- Gets the size of data
        post_data = self.rfile.read(content_length)  # <--- Gets the data itself
        self._set_headers()
        json_obj = json.loads(post_data)
        # self.wfile.write("<html><body><h1>POST!</h1><pre>" + str(json_obj) + "</pre></body></html>")
        self.logger.debug("Received heartbeat object {}".format(str(json_obj)))
        try:
            self.heartbeat_table.on_slave_heartbeat(json_obj['hostname'])
        except Exception as ex:
            self.logger.exception(ex)


# if __name__ == "__main__":
#     from sys import argv
#
#     if len(argv) == 2:
#         run(port=int(argv[1]))
#     else:
#         run()
