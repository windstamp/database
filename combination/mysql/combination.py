# encoding=utf-8

from util import *

import mysql.connector
import time

# 准备原始数据
logging.info('Prepare source data.')

conn_src_1 = mysql.connector.connect(host='localhost', port=3306, user='root', password='123456', use_unicode=True)
conn_src_2 = mysql.connector.connect(host='localhost', port=3306, user='root', password='123456', use_unicode=True)
conn_dst = mysql.connector.connect(host='localhost', port=3306, user='root', password='123456', use_unicode=True)

cursor_src_1 = conn_src_1.cursor()
cursor_src_2 = conn_src_2.cursor()
cursor_dst = conn_dst.cursor()

# 清除遗留的原始数据
logging.info('Delete old source data.')

cursor_src_1.execute('DROP DATABASE IF EXISTS db_src_1;')
conn_src_1.commit()

cursor_src_2.execute('DROP DATABASE IF EXISTS db_src_2;')
conn_src_2.commit()

cursor_dst.execute('DROP DATABASE IF EXISTS db_dst;')
conn_dst.commit()

# 初始化原始数据
logging.info('Init source data.')

cursor_src_1.execute('CREATE DATABASE db_src_1 DEFAULT CHARSET utf8 COLLATE utf8_general_ci;')
cursor_src_1.execute('USE db_src_1;')
cursor_src_1.execute('CREATE TABLE user (id VARCHAR(20) PRIMARY KEY, name VARCHAR(20))')
cursor_src_1.execute('INSERT INTO user (id, name) VALUES (%s, %s)', ['17400100001', 'Michael 1'])
cursor_src_1.execute('INSERT INTO user (id, name) VALUES (%s, %s)', ['17400100002', 'Michael 2'])
conn_src_1.commit()

cursor_src_1.execute('CREATE TABLE user2 (id VARCHAR(20) PRIMARY KEY, name VARCHAR(20))')
cursor_src_1.execute('INSERT INTO user2 (id, name) VALUES (%s, %s)', ['17400100001', 'Michael 1'])
cursor_src_1.execute('INSERT INTO user2 (id, name) VALUES (%s, %s)', ['17400100002', 'Michael 2'])
conn_src_1.commit()

cursor_src_2.execute('CREATE DATABASE db_src_2 DEFAULT CHARSET utf8 COLLATE utf8_general_ci;')
cursor_src_2.execute('USE db_src_2;')
cursor_src_2.execute('CREATE TABLE user (id VARCHAR(20) PRIMARY KEY, name VARCHAR(20))')
cursor_src_2.execute('INSERT INTO user (id, name) VALUES (%s, %s)', ['17400200001', 'Joey 1'])
cursor_src_2.execute('INSERT INTO user (id, name) VALUES (%s, %s)', ['17400200002', 'Joey 2'])
conn_src_2.commit()

cursor_src_2.execute('CREATE TABLE user2 (id VARCHAR(20) PRIMARY KEY, name VARCHAR(20))')
cursor_src_2.execute('INSERT INTO user2 (id, name) VALUES (%s, %s)', ['17400200001', 'Joey 1'])
cursor_src_2.execute('INSERT INTO user2 (id, name) VALUES (%s, %s)', ['17400200002', 'Joey 2'])
conn_src_2.commit()

cursor_dst.execute('create database db_dst DEFAULT CHARSET utf8 COLLATE utf8_general_ci;')
cursor_dst.execute('use db_dst;')
cursor_dst.execute('create table user (id varchar(20) primary key, name varchar(20))')
cursor_dst.execute('create table user2 (id varchar(20) primary key, name varchar(20))')
conn_dst.commit()

cursor_src_1.close()
cursor_src_2.close()
cursor_dst.close()

conn_src_1.close()
conn_src_2.close()
conn_dst.close()

# 开始合服
conn_db_src_1 = mysql.connector.connect(host='localhost', port=3306, user='root', password='123456', database='db_src_1', use_unicode=True)
conn_db_src_2 = mysql.connector.connect(host='localhost', port=3306, user='root', password='123456', database='db_src_2', use_unicode=True)
conn_db_dst = mysql.connector.connect(host='localhost', port=3306, user='root', password='123456', database='db_dst', use_unicode=True)

cursor_db_src_1 = conn_db_src_1.cursor()
cursor_db_src_2 = conn_db_src_2.cursor()
cursor_db_dst = conn_db_dst.cursor()

tbl_names = list()
tbl_names.append('user')
tbl_names.append('user2')

logging.info('')
logging.info('checking before combination start...')
for tbl_name in tbl_names:
    check_tbl_before_combination(cursor_db_src_1, cursor_db_src_2, cursor_db_dst, conn_db_dst, tbl_name)
logging.info('checking before combination end!!!')

logging.info('')
logging.info('combination start...')
for tbl_name in tbl_names:
    combinate_tbl_by_name(cursor_db_src_1, cursor_db_src_2, cursor_db_dst, conn_db_dst, tbl_name, False)
logging.info('combination end!!!')

logging.info('')
logging.info('checking after combination start...')
for tbl_name in tbl_names:
    check_tbl_after_combination(cursor_db_src_1, cursor_db_src_2, cursor_db_dst, tbl_name)
logging.info('checking after combination  end!!!')

cursor_db_src_1.close()
cursor_db_src_2.close()
cursor_db_dst.close()

conn_db_src_1.close()
conn_db_src_2.close()
conn_db_dst.close()
