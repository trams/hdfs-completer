#!/usr/bin/python

import hdfs.util
import tornado.web

from tornado.options import define, options

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
    def __init__(self):
        self.root_node = None
        self.client = None

    def index(self, max_depth=5):
        self.root_node = read_hdfs_tree(self.client, '', max_depth)


#############################################################

class Node(object):
    def __init__(self, name, is_folder, children):
        self.name = name
        self.is_folder = is_folder
        self.children = children


def list_folder(client, path):
    return {name: Node(name, status['type'] == 'DIRECTORY', {})  for name, status in client.list(path, status=True)}


def read_hdfs_tree(client, path, max_depth):
    if max_depth == 0:
        return {}

    real_path = path
    if real_path == "":
        real_path = "/"
    result = list_folder(client, real_path)
    for name, value in result.iteritems():
        try:
            if value.is_folder:
                value.children.update(read_hdfs_tree(client, path + '/' + value.name,  max_depth - 1))
        except hdfs.util.HdfsError:
            #log
            pass
    return result


def print_hdfs_tree(children, offset=0):
    for child in children.itervalues():
        print ' '*offset + child.name
        print_hdfs_tree(child.children, offset + 2)


def get_completetions(root_children, path):
    current = root_children
    basename, filename = path.rsplit('/', 1)
    parts = filter(lambda x: x != '',  basename.split('/'))

    for name in parts:
        if not current.has_key(name):
            return []
        current = current[name].children

    return [basename + '/' + name for name in current if name.startswith(filename)]

#############################################################

def get_client(host='http://192.168.33.10:50070'):
    from hdfs.client import Client
    return Client(host)


def smoke_test():
    root_children = read_hdfs_tree(client, '', 7)
    print_hdfs_tree(root_children)
    print get_completetions(root_children, '/t')
    print get_completetions(root_children, '/tmp/t')
    print get_completetions(root_children, '/notexist')


define("port", default=8888, help="port to listen")
define("depth", default=5, help="max depth to index")
define("hdfs_host", default="http://192.168.33.10:50070", help="hdfs host to index")


if __name__ == "__main__":
    tornado.options.parse_command_line()
    state = State()
    state.client = get_client(options.hdfs_host)
    state.index(options.depth)
    application = tornado.web.Application([
        (r"/v1/completetions", GetCompletetionsHandler, dict(state=state)),
        (r"/v1/reindex", ReIndexHandler, dict(state=state)),
    ])
    application.listen(options.port)
    tornado.ioloop.IOLoop.current().start()
