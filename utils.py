from urllib.parse import unquote
from copy import deepcopy


def json_tree_traversal(tree):
    metric_list = sub_traverse_tree(tree)
    return metric_list


def sub_traverse_tree(root):
    metric = {}
    metrics = []
    for key in list(root):
        if not isinstance(root[key], dict) and not isinstance(root[key], list):
            metric[key] = root[key]
            root.pop(key, None)
    for key in list(root):
        if isinstance(root[key], dict):
            sub_dict_traverse_tree(root[key],metric, metrics)
        if isinstance(root[key], list):
            for sub_node in root[key]:
                sub_dict_traverse_tree(sub_node,metric, metrics)
    return metrics


def sub_dict_traverse_tree(root, metric, metrics):
    leaf = False
    for key in list(root):
        if not isinstance(root[key], dict) and not isinstance(root[key], list):
            metric[key] = root[key]
            root.pop(key, None)
            leaf = True
    for key in list(root):
        if isinstance(root[key], dict):
            leaf = False
            sub_dict_traverse_tree(root[key],metric, metrics)
        if isinstance(root[key], list):
            leaf = False
            for sub_node in root[key]:
                sub_dict_traverse_tree(sub_node,metric, metrics)
    if leaf:
        metrics.append(deepcopy(metric))
    return metric


def create_measurement_name(yang):
    url = unquote(yang)
    url = url.replace('/','-').replace(':', '-').strip('-')
    return url


def make_line_format(measurement, tags, fields):
    rc_str = ''
    rc_str = rc_str + measurement
    for tag_key in tags.keys():
        rc_str += ','
        rc_str += '{}=\"{}\"'.format(tag_key, tags[tag_key])
    rc_str += ' '
    for field_key in fields:
        rc_str += '{}={},'.format(field_key, fields[field_key])
    return rc_str.strip(',')


def format_mdt_output(output):
    print(output)





