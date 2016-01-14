#!/usr/bin/python

import hdfs.util
import tornado
import tornado.web
from tornado.options import define, options, parse_command_line

import logging
import os.path

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



class State(object):
    def __init__(self):
        self.client = None

    def get_completions(self, path):
        basename, filename = parse_path(path)
        return get_completions(self.client, basename, filename)

#############################################################

def get_completions(client, path, prefix):
    return [path + '/' + name + get_suffix(status) for name, status in client.list(path, status=True) if name.startswith(prefix)]

def get_suffix(status):
    if status['type'] == 'DIRECTORY':
        return '/'
    else:
        return ''

def parse_path(not_complete_path):
    try:
        basename, filename = not_complete_path.rsplit('/', 1)
        return (basename, filename)
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


define("port", default=8888, help="port to listen")
define("hdfs_host", default="http://192.168.33.10:50070", help="hdfs host to index")
define("local_host", default="127.0.0.1", help="host to bind")
define("use_kerberos", default=False, help="use kerberos to authenticate")


if __name__ == "__main__":
    parse_command_line()

    state = State()
    state.client = get_client(options.hdfs_host, options.use_kerberos)

    application = tornado.web.Application([
        (r"/v1/completetions", GetCompletetionsHandler, dict(state=state)),
    ])
    application.listen(options.port, address=options.local_host)
    tornado.ioloop.IOLoop.current().start()
