import pymongo


DB_info = {
    "host": "",
    "port": '',
    "user": "",
    "pwd": ""
}


def connection_mongodb():
    connection = pymongo.MongoClient(DB_info['host'], DB_info['port'])
    connection.admin.authenticate(DB_info['user'], DB_info['pwd'])
    return connection
