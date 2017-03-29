import psycopg2
import sys
import os

class DBUtil(object):

	def __init__(self):
		self.connnection = psycopg2.connect("dbname=postgres user=postgres password=test")


	def insert(self, query):
		cursor = self.connnection.cursor()
		cursor.execute(query)
		cursor.close()


	def query(self, query):
		cursor = self.connnection.cursor()
		cursor.execute(query)
		result = cursor.fetchall()
		cursor.close()
		
		return result


	def __del__(self):
		self.connnection.commit()
		self.connnection.close()
