import completer


def create_file(name):
    return completer.Node(name, False, {})


def create_folder(name, children):
    return completer.Node(name, True, {child.name: child for child in children})



ROOT=create_folder(
    "/",[
        create_folder("tmp", [
            create_file("some_file"),
            create_file("other_file"),
            create_folder("subfolder", [])
            ]),
        create_folder("var", []),
        create_file("t_file_in_root")
    ])

completer.print_hdfs_tree(ROOT.children)


def test_folder_end_with_slash():
    assert completer.get_completetions(ROOT.children, "/v") == ["/var/"]

def test_file_without_slash():
    assert completer.get_completetions(ROOT.children, "/t_") == ["/t_file_in_root"]

def test_multiple_choices():
    assert set(completer.get_completetions(ROOT.children, "/t")) == set(["/tmp/", "/t_file_in_root"])

def test_notexisten():
    assert completer.get_completetions(ROOT.children, "/not_existen") == []

def test_bad_input():
    assert completer.get_completetions(ROOT.children, "bad_input") == []



FILE_ATTRIBUTES = dict(type="FILE")
FOLDER_ATTRIBUTES = dict(type="DIRECTORY")
FAKECLIENT_DATA = {
    "/" : [("tmp", FOLDER_ATTRIBUTES), ("var", FOLDER_ATTRIBUTES), ("t_file_in_root", FILE_ATTRIBUTES)],
    "/tmp" : [("some_file", FILE_ATTRIBUTES), ("other_file", FILE_ATTRIBUTES), ("subfolder", FOLDER_ATTRIBUTES)],
    "/tmp/subfolder" : [],
    "/var": []
}

class FakeClient(object):
    def __init__(self, data):
        self._data = data

    def list(self, path, status):
        assert status
        return self._data[path]


CLIENT = FakeClient(FAKECLIENT_DATA)


def test_read_hdfs_tree_basic():
    tree = completer.read_hdfs_tree(CLIENT, "", 10)
    assert completer.get_completetions(tree, "/v") == ["/var/"]
    assert completer.get_completetions(tree, "/t_") == ["/t_file_in_root"]
    assert set(completer.get_completetions(tree, "/t")) == set(["/tmp/", "/t_file_in_root"])
    assert completer.get_completetions(tree, "/not_existen") == []
    assert completer.get_completetions(tree, "bad_input") == []
