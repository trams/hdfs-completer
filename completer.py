#!/usr/bin/python

import hdfs.util
import tornado.web
import logging

from tornado.options import define, options, parse_command_line

LOG = logging.getLogger("completer")

#############################################################

class GetCompletetionsHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, state):
        super(GetCompletetionsHandler, self).__init__(application, request)
        self._state = state

    def get(self):
        path = self.get_argument("path")
        completetions = get_completetions(self._state.root_node, path)
        self.write("\n".join(completetions))


class ReIndexHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, state):
        super(ReIndexHandler, self).__init__(application, request)
        self._state = state

    def get(self):
        max_depth = int(self.get_argument("max_depth", 4))
        self._state.index(max_depth)


class State(object):
    def __init__(self, skip_list):
        self.skip_list = skip_list
        self.root_node = None
        self.client = None

    def index(self, max_depth=5):
        self.root_node = read_hdfs_tree(self.client, '', max_depth, skip_list)


#############################################################

class Node(object):
    def __init__(self, name, is_folder, children):
        self.name = name
        self.is_folder = is_folder
        self.children = children


def list_folder(client, path):
    return {name: Node(name, status['type'] == 'DIRECTORY', {})  for name, status in client.list(path, status=True)}


def read_hdfs_tree(client, path, max_depth, skip_list=[]):
    if max_depth == 0:
        return {}

    real_path = path
    if real_path == "":
        real_path = "/"
    result = list_folder(client, real_path)
    for name, value in result.iteritems():
        try:
            if name.strip() in skip_list:
                LOG.debug("%s in the skip list. Skip", name)
                continue
            if value.is_folder:
                value.children.update(read_hdfs_tree(client, path + '/' + value.name,  max_depth - 1, skip_list))
        except hdfs.util.HdfsError:
            #log
            pass
    return result


def print_hdfs_tree(children, offset=0):
    for child in children.itervalues():
        print ' '*offset + child.name
        print_hdfs_tree(child.children, offset + 2)


def get_completetions(root_children, path):
    if not path.startswith("/"):
        return []

    current = root_children
    basename, filename = path.rsplit('/', 1)
    parts = filter(lambda x: x != '',  basename.split('/'))

    for name in parts:
        if not current.has_key(name):
            return []
        current = current[name].children

    return [
        _merge_paths(basename, name, attrs.is_folder)
        for name, attrs in current.iteritems()
        if name.startswith(filename)
    ]

def _merge_paths(basename, name, is_folder):
    if is_folder:
        return basename + '/' + name + '/'
    else:
        return basename + '/' + name


#############################################################

def get_client(host, use_kerberos):
    if use_kerberos:
        from hdfs.ext.kerberos import KerberosClient
        return KerberosClient(host)
    else:
        from hdfs.client import Client
        return Client(host)


define("port", default=8888, help="port to listen")
define("depth", default=5, help="max depth to index")
define("hdfs_host", default="http://192.168.33.10:50070", help="hdfs host to index")
define("local_host", default="127.0.0.1", help="host to bind")
define("use_kerberos", default=False, help="use kerberos to authenticate")
define("skip_list_file", help="skip_list_file")


if __name__ == "__main__":
    parse_command_line()

    if "skip_list_file" in options:
        LOG.debug("Load a skip list from %s", options.skip_list_file)
        with open(options.skip_list_file) as f:
            skip_list = map(lambda line: line.strip(), f.readlines())
    else:
        skip_list = []

    state = State(skip_list)
    state.client = get_client(options.hdfs_host, options.use_kerberos)
    state.index(options.depth)

    application = tornado.web.Application([
        (r"/v1/completetions", GetCompletetionsHandler, dict(state=state)),
        (r"/v1/reindex", ReIndexHandler, dict(state=state)),
    ])
    application.listen(options.port, address=options.local_host)
    tornado.ioloop.IOLoop.current().start()
