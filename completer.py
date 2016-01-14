#!/usr/bin/python

import hdfs.util
import tornado
import tornado.web
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
from tornado.options import define, options, parse_command_line

import logging
import json

LOG = logging.getLogger("completer")

#############################################################

class CancelReIndex(StandardError):
    pass


class GetCompletetionsHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, state):
        super(GetCompletetionsHandler, self).__init__(application, request)
        self._state = state

    def get(self):
        path = self.get_argument("path")
        completetions = get_completetions(self._state.root_node, path)
        self.write("\n".join(completetions))


class ReIndexHandler(tornado.web.RequestHandler):
    executor = ThreadPoolExecutor(max_workers=1)

    def __init__(self, application, request, state):
        super(ReIndexHandler, self).__init__(application, request)
        self._state = state

    @tornado.gen.coroutine
    def get(self):
        try:
            max_depth = int(self.get_argument("max_depth", 4))
            self._state._root_node_future = self.reindex(max_depth)
            self._state.root_node = yield self._state._root_node_future
            self.finish()
        except Exception:
            LOG.error("Failed to reindex", exc_info=True)
            raise

    @run_on_executor
    def reindex(self, max_depth):
        return self._state.reindex(max_depth)


class CancelReIndexHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, state):
        super(ReIndexHandler, self).__init__(application, request)
        self._state = state

    def get(self):
        if self._state._root_node_future is not None:
            self._state._root_node_future.set_exception(CancelReIndex)
            self._state._root_node_future = None
        else:
            self.set_status(500, reason="There is no reindex in progress. Nothing to cancel")


class SaveHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, state):
        super(SaveHandler, self).__init__(application, request)
        self._state = state

    def get(self):
        filename = self.get_argument("filename")
        self._state.save(filename)


class LoadHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, state):
        super(LoadHandler, self).__init__(application, request)
        self._state = state

    def get(self):
        filename = self.get_argument("filename")
        self._state.load(filename)


class State(object):
    def __init__(self, skip_list):
        self.skip_list = skip_list
        self.root_node = None
        self.client = None
        self._root_node_future = None

    def save(self, filename):
        with open(filename, "w") as f:
            json.dump({name: value.to_raw() for name, value in self.root_node.iteritems()}, f)

    def load(self, filename):
        with open(filename) as f:
            root = json.load(f)
            for key in root:
                root[key] = from_raw(root[key])
            self.root_node = root

    def reindex(self, max_depth=5):
        return read_hdfs_tree(self.client, '', max_depth, self.skip_list)

    def index(self, max_depth):
        self.root_node = self.reindex(max_depth)

#############################################################

class Node(object):
    def __init__(self, name, is_folder, children):
        self.name = name
        self.is_folder = is_folder
        self.children = children

    def to_raw(self):
        return dict(
            name=self.name,
            is_folder=self.is_folder,
            children=[child.to_raw() for child in self.children.itervalues()])

    def __eq__(self, other):
        return self.name == other.name and self.is_folder == other.is_folder and self.children == other.children


_KEYS=["name", "is_folder", "children"]
def from_raw(raw):
    for key in _KEYS:
        if not key in raw:
            raise ValueError("Expected the raw to contain the " + key);
    return Node(
        raw["name"],
        raw["is_folder"],
        to_indexed([from_raw(item) for item in raw["children"]]))

def to_indexed(iterable):
    return {item.name: item for item in iterable}



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
        (r"/v1/cancelreindex", CancelReIndexHandler, dict(state=state)),
        (r"/v1/save", SaveHandler, dict(state=state)),
        (r"/v1/load", LoadHandler, dict(state=state))
    ])
    application.listen(options.port, address=options.local_host)
    tornado.ioloop.IOLoop.current().start()
