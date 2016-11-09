#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#引入模块
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import datetime

#将信息写入数据库
def insertDB(db_name,table_name,key_str,value):
	conn = MySQLdb.connect( host='localhost',user='root',passwd='',charset="utf8")
	conn.select_db(db_name)
	sql = "INSERT INTO %s(%s) values(%s)" %(table_name,key_str,value)
	try:
		cursor = conn.cursor()
		cursor.execute(sql)
		cursor.rowcount
		conn.commit()
		cursor.close()
		print('导入一条')
	except :
		print('未能导入数据库')
	conn.rollback()
	conn.close()

#读取进货宝csv文件信息
def getJhbInfo(filename,db_name,table_name,key_str):
	with open(filename, 'r') as f:
		first = 215
		for info in f:
			if first <= 215:
				first += 1
			else:
				infos = info.split(',')
				key = infos[1]
				zone_id = 1000
				pname = infos[2]
				order_price = float(infos[3])
				if not infos[4]:
					origin_price = ''
				else:
					origin_price = float(infos[4])
				spec = ''
				unit_name = infos[5]
				code = ''
				img_url = infos[8]
				if infos[7].find('起订')>0:
					min_num = int(infos[7][:1])
				else:
					min_num = ''
				fetch_time = int(infos[9])
				promotion = infos[6]
				state = infos[0]
				insert_infos = [key,zone_id,pname,order_price,origin_price,spec,unit_name,code,img_url,min_num,fetch_time,promotion,state]
				insert_infos = str(insert_infos).replace('[','')
				insert_infos = insert_infos.replace(']','')
				insertDB(db_name,table_name,key_str,insert_infos)

#读取店商互联csv文件信息
def getDsInfo(filename,db_name,table_name,key_str):
	with open(filename, 'r') as f:
		first = 0
		for info in f:
			if first == 0:
				first += 1
			else:
				infos = info.split(',')
				key = infos[0]
				zone_id = 1000
				pname = infos[1]
				order_price = float(infos[4])
				origin_price = float(infos[5])
				spec = infos[7]
				unit_name = ''
				code = infos[3]
				img_url = infos[6]
				min_num = ''
				fetch_time = int(infos[8])
				brand = infos[2]
				insert_infos = [key,zone_id,pname,order_price,origin_price,spec,unit_name,code,img_url,min_num,fetch_time,brand]
				insert_infos = str(insert_infos).replace('[','')
				insert_infos = insert_infos.replace(']','')
				insertDB(db_name,table_name,key_str,insert_infos)

#读取全时汇csv文件信息
def getQshInfo(filename,db_name,table_name,key_str):
	with open(filename, 'r') as f:
		first = 0
		for info in f:
			if first == 0:
				first += 1
			else:
				infos = info.split(',')
				key = infos[0]
				zone_id = 1000
				pname = infos[1]
				order_price = float(infos[2])
				origin_price = ''
				spec = infos[3]
				unit_name = infos[4]
				code = ''
				img_url = infos[8]
				min_num = infos[5]
				fetch_time = int(infos[9])
				max_num = infos[6]
				order_spec = infos[7]
				insert_infos = [key,zone_id,pname,order_price,origin_price,spec,unit_name,code,img_url,min_num,fetch_time,max_num,order_spec]
				insert_infos = str(insert_infos).replace('[','')
				insert_infos = insert_infos.replace(']','')
				insertDB(db_name,table_name,key_str,insert_infos)
day = str(datetime.date.today()).replace('-','')

#写入进货宝
filename = '/Users/alex/Desktop/jhb_'+ day +'.csv'
db_name = 'spider'
table_name = 'spi_jinhuobao'
key_str = '`key`,zone_id,pname,order_price,origin_price,spec,unit_name,code,img_url,min_num,fetch_time,promotion,state'
#getJhbInfo(filename,db_name,table_name,key_str)

#写入店商互联
filename = '/Users/alex/Desktop/ds_'+ day +'.csv'
db_name = 'spider'
table_name = 'spi_ds365'
key_str = '`key`,zone_id,pname,order_price,origin_price,spec,unit_name,code,img_url,min_num,fetch_time,brand'
#getDsInfo(filename,db_name,table_name,key_str)

#写入全时汇库
filename = '/Users/alex/Desktop/qsh_'+ day +'.csv'
db_name = 'spider'
table_name = 'spi_quanshihui'
key_str = '`key`,zone_id,pname,order_price,origin_price,spec,unit_name,code,img_url,min_num,fetch_time,max_num,order_spec'
getQshInfo(filename,db_name,table_name,key_str)