#!/usr/bin/python3
# -*- coding:utf-8 -*-
# createtime: 2020/11/14 11:08
import threading
import websocket
import time
import pandas as pd
import pymysql
import datetime
from openpyxl import load_workbook

#客户端, 使用websocket连接
class DyBarrageWebsocketClient:

    def __init__(self, on_open, on_message, on_close):
        self.__url = 'wss://danmuproxy.douyu.com:8506/'

        self.__websocket = websocket.WebSocketApp(self.__url,
                          on_open=on_open, on_message=on_message,
                          on_error=self.__on_error, on_close=on_close)
    #启动客户端
    def start(self):
        self.__websocket.run_forever()
    #关闭客户端
    def stop(self):
        self.__websocket.close()
    #发送消息
    def send(self, msg):
        self.__websocket.send(msg)
    #错误处理
    def __on_error(self, err):
        print(err)

#消息处理
class DyBarrageRawMsgHandler:

    def dy_encode(self, msg):

        data_len = len(msg) + 9
        # value = 客户端发送给弹幕服务器的文本格式数据
        value = 689
        msg_byte = msg.encode('utf-8')
        # 消息长度
        len_byte = int.to_bytes(data_len, 4, 'little')
        #消息类型
        send_byte = int.to_bytes(value, 4, 'little')
        #尾部
        end_byte = bytearray([0x00])

        data = len_byte + len_byte + send_byte + msg_byte + end_byte

        return data

    def dy_decode(self, msg_byte):
        pos: int = 0
        msg: list = []

        while pos < len(msg_byte):
            #消息长度
            content_length = int.from_bytes(msg_byte[pos:pos+4], byteorder='little')

            content = msg_byte[pos + 12: pos + content_length].decode(encoding='utf-8', errors='ignore')
            msg.append(content)
            pos += (4+content_length)
        return msg

    def get_chat_messages(self, msg_byte):

        decode_msg = self.dy_decode(msg_byte)

        messages = []
        for msg in decode_msg:
            res = self.__parse_msg(msg)
            if res['type'] != 'chatmsg':
                continue
            messages.append(res)
        return messages
    def __parse_msg(self, raw_msg):
        res = {}
        attrs = raw_msg.split('/')[0:-1]

        for attr in attrs:

            couple = attr.split('@=')
            res[couple[0]] = couple[1]

        return res


class _DyBarrageDBHandler:
    def __init__(self):
        self.__conn = None
        self.__cursor = None
        self.__db_name = 'dybarrage'
        self.__table_name = 'barrages'
        self.__cols = ['rid', 'uid', 'cid', 'nn', 'level', 'txt', 'stime', 'ic']
        self.__cols = [
            ['rid', 'varchar(10) not null'],
            ['uid', 'varchar(20) not null'],
            ['cid', 'varchar(32) not null'],
            ['nn', 'varchar(60) not null'],
            ['level', 'int default 0 not null'],
            ['txt', 'varchar(200) not null'],
            ['stime', 'datetime not null'],
            ['ic', 'varchar(200) not null']
        ]

    def connect(self):
        self.__conn = pymysql.connect(host='127.0.0.1', user='root', password='pangde', charset='utf8')
        self.__cursor = self.__conn.cursor()

    def __create_db(self):
        self.__cursor.execute('create database if not exists %s;' % self.__db_name)
        self.__cursor.execute('use %s;' % self.__db_name)
        self.__conn.commit()

    def __create_table(self):
        sql = 'create table if not exists %s (\n' % self.__table_name
        for col in self.__cols:
            sql += col[0] + ' ' + col[1] + ',\n'
        sql = sql[0:-2] + '\n);\n'

        self.__cursor.execute(sql)
        self.__conn.commit()

    def prepare(self):
        self.__create_db()
        self.__create_table()

    def disconnect(self):
        self.__cursor.close()
        self.__conn.close()

    def insert_barrage(self, barrage):
        insert_col_sql = 'insert into %s(' % self.__table_name
        insert_value_sql = 'values('

        barrage['stime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for k in barrage.keys():
            for col in self.__cols:
                if k == col[0]:
                    insert_col_sql += k + ', '
                    if 'int' in col[1]:
                        insert_value_sql += barrage[k] + ', '
                    else:
                        insert_value_sql += '\'' + barrage[k].replace('\'', '') + '\', '

        sql = insert_col_sql[0:-2] + ') ' + insert_value_sql[0:-2] + ');'

        self.__cursor.execute(sql)
        self.__conn.commit()


dy_barrage_db_handler = _DyBarrageDBHandler()
dy_barrage_raw_msg_handler = DyBarrageRawMsgHandler()
class DyBarrageCrawler:
    def __init__(self, room_id):
        self.__room_id = room_id
        self.__heartbeat_thread = None
        self.__clent = DyBarrageWebsocketClient(on_open=self.__prepare, on_message=self.__recive_msg, on_close=self.__stop)
        self.__msg_handler = dy_barrage_raw_msg_handler
        self.__db_handler = dy_barrage_db_handler
        self.__should_stop_heartbeat = False

    def start(self):
        self.__db_handler.connect()
        self.__db_handler.prepare()
        self.__clent.start()

    def __stop(self):
        self.__logout()
        self.__clent.stop()
        self.__db_handler.disconnect()
        self.__should_stop_heartbeat = True

    def __prepare(self):
        self.__login()
        self.__join_group()
        self.__start_heartbeat()

    def __login(self):
        login_msg = 'type@=loginreq/roomid@={}/'.format(self.__room_id)
        self.__clent.send(self.__msg_handler.dy_encode(login_msg))
    def __join_group(self):
        join_group_msg = 'type@=joingroup/rid@={}/gid@=-9999/'.format(self.__room_id)
        self.__clent.send(self.__msg_handler.dy_encode(join_group_msg))

    def __recive_msg(self, msg):

        chat_messages = self.__msg_handler.get_chat_messages(msg)
        self.__download_msg(chat_messages)
        for chat_message in chat_messages:
            print(chat_messages)
            self.__db_handler.insert_barrage(chat_message)

    def __download_msg(self, chat_messages):
        # columns = ['房间号', '用户ID', '用户昵称', '消息内容', '用户等级', '发送时间']
        data = []
        for chat_message in chat_messages:
            item = [chat_message['rid'], chat_message['uid'], chat_message['nn'], chat_message['txt'], chat_message['level'], chat_message['cst']]
            data.append(item)
        df = pd.DataFrame(data)
        df.to_csv('danmu.csv', mode='a', header=None, index=False, encoding='utf-8')

    def __send_dmmsg(self):
        send_msg = self.__msg_handler.dy_encode('66')
        self.__clent.send()

    def __start_heartbeat(self):
        self.__heartbeat_thread = threading.Thread(target=self.__heartbeat)
        self.__heartbeat_thread.start()
    def __heartbeat(self):
        heartbeat_msg = 'type@=keeplive/tick@=1439802131/'
        msg_byte = self.__msg_handler.dy_encode(heartbeat_msg)

        while True:
            self.__clent.send(msg_byte)
            for i in range(90):
                time.sleep(0.5)
                if self.__should_stop_heartbeat:
                    return

    def __logout(self):
        logout_msg = 'type@=logout/'
        logout_msg_byte = self.__msg_handler.dy_encode(logout_msg)

if __name__ == '__main__':
    crawler = DyBarrageCrawler('1126960')
    crawler.start()