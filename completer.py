import hdfs.util


class Node(object):
    def __init__(self, name, is_folder, children):
        self.name = name
        self.is_folder = is_folder
        self.children = children


def list_folder(client, path):
    return {name: Node(name, status['type'] == 'DIRECTORY', {})  for name, status in client.list(path, status=True)}


def read_hdfs_tree(client, path, max_depth):
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
        current = current[name].children

    return [basename + '/' + name for name in current if name.startswith(filename)]


if __name__ == "__main__":
    from hdfs.client import Client
    client = Client('http://192.168.33.10:50070')
    root_children = read_hdfs_tree(client, '', 7)
    print_hdfs_tree(root_children)
    print get_completetions(root_children, '/t')
    print get_completetions(root_children, '/tmp/t')
    print get_completetions(root_children, '/notexist')
