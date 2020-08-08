"""
DB 정보 저장 파일
절대 외부 업로드 금지
"""
import pymysql

DBinfo = {
    'user': '',
    'passwd': '',
    'host': '',
    'port': '',
    'charset': 'utf8'
}


def connection_db():
    DB_Connection = pymysql.connect(
        user=DBinfo['user'],
        passwd=DBinfo['passwd'],
        host=DBinfo['host'],
        port=DBinfo['port'],
        db='news',
        charset=DBinfo['charset']
    )
    return DB_Connection
