#!/usr/bin/python

import hdfs.util
import tornado
import tornado.web
from tornado.options import define, options, parse_command_line
from tornado import netutil
from tornado import httpserver

import logging
import os.path
import time
import sys

LOG = logging.getLogger("completer")

#############################################################

class GetCompletetionsHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, state):
        super(GetCompletetionsHandler, self).__init__(application, request)
        self._state = state

    def get(self):
        path = self.get_argument("path")
        completions = self._state.get_completions(path)
        self.write("\n".join(completions))


class ListHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, state):
        super(ListHandler, self).__init__(application, request)
        self._state = state

    def get(self):
        path = self.get_argument("path")
        try:
            directory_list = self._state.get_list(path)
            self.write("\n".join(directory_list))
        except hdfs.util.HdfsError as e:
            self.write(str(e) + "\n")


class State(object):
    def __init__(self, client):
        self.client = client
        self._cache = {}

    def get_list(self, path):
        directory_content = self.fetch(path)
        return [name + get_suffix(status) for name, status in directory_content]

    def get_completions(self, path):
        basename, filename = parse_path(path)
        entry = self._cache.get(basename)
        if entry is None:
            directory_content = self.fetch(basename)
        else:
            ts, directory_content = entry
            if ts + 3600 < time.time():
                directory_content = self.fetch(basename)
        return [os.path.join(basename, name) + get_suffix(status) for name, status in directory_content if name.startswith(filename)]

    def fetch(self, path):
        LOG.info("There is no '%s' content in the cache. Fetch...", path)
        directory_content = self.client.list(path, status=True)
        self._cache[path] = (time.time(), directory_content)
        return directory_content



#############################################################

def get_suffix(status):
    if status['type'] == 'DIRECTORY':
        return '/'
    else:
        return ''

def parse_path(not_complete_path):
    try:
        basename, filename = not_complete_path.rsplit('/', 1)
        if basename:
            return (basename, filename)
        else:
            return ('/', filename)
    except ValueError:
        return ('/', '')

#############################################################

def get_client(host, use_kerberos):
    if use_kerberos:
        from hdfs.ext.kerberos import KerberosClient
        return KerberosClient(host)
    else:
        from hdfs.client import Client
        return Client(host)


def launch_server():
    state = State(get_client(options.hdfs_host, options.use_kerberos))

    application = tornado.web.Application([
        (r"/v1/completetions", GetCompletetionsHandler, dict(state=state)),
        (r"/v1/list", ListHandler, dict(state=state)),
    ])
    if options.unix_socket:
        sockets = [netutil.bind_unix_socket(options.unix_socket)]
    elif options.port_file:
        sockets = netutil.bind_sockets(0, address=options.local_host)
        port = sockets[0].getsockname()[1]
        with open(options.port_file, "w+") as port_file:
            port_file.write(str(port))
    else:
        sys.stderr.write("Use either --unix_socket or --port_file\n")
        return

    server = httpserver.HTTPServer(application)
    server.add_sockets(sockets)
    tornado.ioloop.IOLoop.current().start()


def list_folder():
    state = State(get_client(options.hdfs_host, options.use_kerberos))
    directory_list = state.get_list(options.list)
    sys.stdout.write("\n".join(directory_list))


define('unix_socket', group='webserver', default=None, help='Path to unix socket to bind')
define('port_file', default=None, help='Path to a file with a port number to dump')
define("hdfs_host", default="http://192.168.33.10:50070", help="hdfs host to index")
define("local_host", default="127.0.0.1", help="host to bind")
define("use_kerberos", default=False, help="use kerberos to authenticate")
define("list", default=None, help="list the content of the FOLDER")


if __name__ == "__main__":
    parse_command_line()

    if options.list:
        list_folder()
    else:
        launch_server()
