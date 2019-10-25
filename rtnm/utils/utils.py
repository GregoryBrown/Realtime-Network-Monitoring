import re
from protos.gnmi_pb2 import PathElem, Path


def create_gnmi_path(path):
    path_elements = []
    if path[0] == '/':
        if path[-1] == '/':
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[1:-1]
        else:
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[1:]
    else:
        if path[-1] == '/':
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)[:-1]
        else:
            path_list = re.split(r'''/(?=(?:[^\[\]]|\[[^\[\]]+\])*$)''', path)
    for elem in path_list:
        elem_name = elem.split("[", 1)[0]
        elem_keys = re.findall(r'\[(.*?)\]', elem)
        dict_keys = dict(x.split('=', 1) for x in elem_keys)
        path_elements.append(PathElem(name=elem_name, key=dict_keys))
    return Path(elem=path_elements)
    

