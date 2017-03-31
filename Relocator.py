from __future__ import print_function

from shutil import copyfile
import os
import sys
from Util import DiskUtil


""" Only one instance of the class would be created 
	and all methods are static methods """
class TravelAgent(object):

	def __init__(self):
		pass

	@staticmethod
	def relocate_file(src_path, dst_path):
		# 1. check if dst has enough space
		available = DiskUtil.get_available_space("/".join(dst_path.split("/")[:-1]))
		if available <= os.path.getsize(src_path):
			print("Available:", available, "File size:", os.path.getsize(src_path))
			# not enough space!
			# 2. call cleanup if not
		else:
			print("We are good! Available:", available, "File size:", os.path.getsize(src_path))

		# 3. if possible, call copy
		copyfile(src_path, dst_path)

		# 4. call symlink 
    	os.remove(src_path)
		os.symlink(dst_path, src_path)



if __name__ == "__main__":
	TravelAgent.relocate_file(sys.argv[1], sys.argv[2])




