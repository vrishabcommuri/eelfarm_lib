from pickle import dumps, loads


def serialize(data, protocol=4):
    return dumps(data, protocol)


def deserialize(data):
    return loads(data)
