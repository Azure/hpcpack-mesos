import json
import logging
import threading
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

import logging_aux
from hpc_cluster_manager import HpcClusterManager


class HeartBeatServer(object):  # TODO: replace this implementation with twisted based implementation
    def __init__(self, heartbeat_table, port=80):
        self.logger = logging_aux.init_logger_aux("hpcframework.heatbeat_server", "hpcframework.heatbeat_server.log")
        self._heartbeat_table = heartbeat_table  # type: 
        self._server_address = ('', port)
        self._server_class = HTTPServer
        self._handler_class = HeartBeatHandler
        self._port = port
        self._httpd = self._server_class(self._server_address, self._handler_class)
        self._server_thread = threading.Thread(target=self.run)
        HeartBeatHandler.logger = self.logger
        HeartBeatHandler.clusmgr = self._heartbeat_table

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
    clusmgr = None  # type: HpcClusterManager

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        self._set_headers()
        self.wfile.write("<html><body><h1>hi from thread {}!</h1></body></html>".format(threading.get_ident()))

    def do_HEAD(self):
        self._set_headers()

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        self._set_headers()
        json_obj = json.loads(post_data)
        self.logger.debug("Received heartbeat object {}".format(str(json_obj)))
        try:
            self.clusmgr.on_slave_heartbeat(json_obj['hostname'])
        except Exception as ex:
            self.logger.exception(ex)
