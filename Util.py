import psycopg2
import sys
import os
from collections import namedtuple


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



class DiskUtil(object):

	@staticmethod
	def get_available_space(path):
	    """
		http://stackoverflow.com/questions/787776/find-free-disk-space-in-python-on-os-x
	    """
	    st = os.statvfs(path)
	    return st.f_bavail * st.f_frsize
	    

