
# tweets.py

import json
import string
import os
import re
import sys
import time
import datetime
import threading
import cPickle as pickle
import numpy as np
from Queue import Queue

class ProcessList():
	'''
	List based functions
	'''

	def __init__(self):
		'''
		Define the data containers
		'''

		# self.status_df = pd.DataFrame()


	def read_data_file(self, data_file, max_lines = 0):
		'''
		Read in the text file up to specified number of lines
		'''
		data_list = []
		line_num = 0
		with open(data_file) as data_file:
			for line in data_file:
				try:
					data = json.loads(line)
					# print(data.keys())
					data_list.append(data)
				except:
					print("There was a problem")
				line_num += 1
				# print(line_num)
				# print(line)
				if line_num == max_lines:
					break

		return data_list

	def get_feature(self, data_list, feature):
		'''
		Print the status if feature is True
		Might also want a get_user_feature
		'''
		count = 0
		for status in data_list:
			count += 1
			if feature in status.keys():
				if status[feature]:
					print(status[feature])
					break
				else:
					print(count)

	def get_keys(self, data_list):
		'''
		Just to make sure I'm getting all the good stuff
		Check the keys for a number of status dicts.
		'''
		status_keys = []
		user_keys = []
		entities_keys = []

		for status in data_list:
			# for key in status.keys():
				# if key not in set(status_keys):
					# print(key, status[key])

			status_keys.extend(status.keys())

			if 'user' in status.keys():
				# for key in status['user'].keys():
				# 	if key not in set(user_keys):
				# 		print(key, status['user'][key])
	
				user_keys.extend(status['user'].keys())

			if 'entities' in status.keys():
				# for key in status['entities'].keys():
				# 	if key not in set(entities_keys):
				# 		print(key, status['entities'][key])

				entities_keys.extend(status['entities'].keys())

		print(set(status_keys))
		# print(set(user_keys))
		# print(set(entities_keys))

	def process_data(self, data_list):
		'''
		This function will create columns from a list of status dicts.
		Much of the information is redundant.
		'''
		# Make columns of interest
		# The map functio has built in ignore... na_action = 'ignore' but not sure it applies
		self.status_df['status_id'] = map(lambda line: line['id'] if 'id' in line.keys() else np.nan, data_list)
		self.status_df['created_at'] = map(lambda line: line['created_at'] if 'created_at' in line.keys() else np.nan, data_list)
		self.status_df['timestamp_ms'] = map(lambda line: line['timestamp_ms'] if 'timestamp_ms' in line.keys() else np.nan, data_list)
		self.status_df['geo'] = map(lambda line: line['geo'] if 'geo' in line.keys() else np.nan, data_list)
		self.status_df['place'] = map(lambda line: line['place'] if 'place' in line.keys() else np.nan, data_list)
		
		# From user dict want id, lang, description/hashtags, location, timezone, created_at, screen_name
		self.status_df['user_id'] = map(lambda line: line['user']['id'] if 'user' in line.keys() else np.nan, data_list)
		self.status_df['user_lang'] = map(lambda line: line['user']['lang'] if 'user' in line.keys() else np.nan, data_list)
		self.status_df['text'] = map(lambda line: line['text'] if 'text' in line.keys() else np.nan, data_list)
		self.status_df['hashtags'] = apply(self.get_entities_hashtags, data_list)

	def print_vals(self, column):
		values = self.status_df[column].dropna()
		print(values)
		print(len(values))

	def get_hashtags_list(self, data_list):
		'''
		collect hashtags from entities and user description
		Parsing the description doesn't fix typos or carriage returns?
		Basically non-alphanumeric tags need to be further inspected
		'''
		hashtags = []
		for line in data_list:
			entities_hashtags = self.get_entities_hashtags(line)
			user_hashtags = self.get_user_hashtags(line)
			# print(entities_hashtags)
			# print(user_hashtags)

			# hashtags.extend(entities_hashtags)
			hashtags.extend(entities_hashtags + user_hashtags)

		return hashtags

class ProcessFile():
	'''
	Each line in each file is just a data record
	'''

	def __init__(self, data_queue, file_path):
		'''
		Takes a data queue
		'''
		self.q = data_queue

		self.file_path = file_path

		self._maxlines = 0

	def set_maxlines(self, max_lines):
		'''
		Simple setter for max lines read from file
		'''
		self._maxlines = maxlines
	
	def start_file_thread(self):
		'''
		Start the thread that sends lines to the queue
		'''

		# Thread kwargs are sent as a dict... weird
		# Also, args must be a sequence, thus the trailing comma... double weird
		self.ft = threading.Thread(target = self.process_file,
								   args = (self.file_path,),
								   kwargs = {'max_lines': self._maxlines})

		self.ft.start()

		return True

	def stop(self):
		'''
		Just join the thread
		Main thread will block here until file is complete
		'''

		self.ft.join()

		return True

	def process_dir(self, dir):
		'''
		Process a directory
		'''

	def process_file(self, data_file, max_lines = 0):
		'''
		Process the text file up to specified number of lines
		Set max_lines = 0 to process entire file
		'''
		line_num = 0
		# status_list = []
		with open(data_file) as data_file:
			for line in data_file:
				# Queue.put() will block if full
				# print(self.q.qsize())

				self.q.put(line, timeout = 1)
				# self.enqueue(line)
				# self.process_line(line)
				line_num += 1
				sys.stdout.write('Processing status: %d\r' % line_num)
				# Not entirely sure we need the flush...
				sys.stdout.flush()
				if line_num == max_lines:
					break

		# return pd.DataFrame(status_list)
		return True

class ProcessQueue():

	def __init__(self, data_queue, data_dir):
		'''
		Create a dataframe
		Makes sense to take the data queue
		'''

		# The data queue to read from
		self.q = data_queue

		self.data_dir = data_dir

		# Maybe this should be it's own file writing class
		self.list_q = Queue(2)

		# The list to write status dicts to.
		# Apparently Python lists are thread safe, maybe use futures?
		self.status_list = []

		self.status_count = 0

		# The run Event
		self.run_p = threading.Event()
		self.run_w = threading.Event()

	def start_line_thread(self):
		'''
		Start process thread
		'''

		self.pt = threading.Thread(target = self.get_line, args = (self.run_p,))

		# Must set the run Event to be True
		self.run_p.set()
		self.pt.start()

		return True

	def start_list_thread(self):
		'''
		Start the file writng thread
		'''

		self.wt = threading.Thread(target = self.get_list, args = (self.run_w,))

		# Set the writer run flag to True
		self.run_w.set()
		self.wt.start()

		return True

	def stop(self):
		'''
		Tell the queue thread stop running when done and join
		'''

		# Set the run state to false
		self.run_p.clear()
		self.run_w.clear()

		# Main thread will block here until queues are empty
		sys.stdout.write('\n')
		print('Joining threads')
		self.pt.join()
		self.wt.join()

		# As a final step, save remaining list
		self.save_list(self.status_list)
		print('Processing complete.')

		return True

	def get_line(self, run_flag):
		'''
		Get lines from data queue
		The run_flag arg is a threading.Event()
		'''
		# lines = 0
		while True:
			if not self.q.empty():
				# Queue.get() will block.  Might need to do a try, except
				# print(self.q.qsize())
				line = self.q.get()
				self.process_line(line)
				# This call reduces the unfinished task count (built in to Queue)
				# Look into q.join instead of flags
				self.q.task_done()
				# lines += 1
			elif run_flag.is_set():
				# print(run_flag.is_set())
				# print('sleeping')
				time.sleep(0.001)
			else:
				print("Line thread exiting")

				return True

		return False

	def process_line(self, line):
		'''
		Convert line to json dict
		Send status to queue
		'''

		status = json.loads(line)
		# Check if there's a quoted status to process (about 1/6)
		if 'is_quote_status' in status.keys():
			if status['is_quote_status']:
				# The quoted status is a full attached status dict
				# The retweeted_status is also a full status dict, but with same text.
				# It might be useful to keep track of retweets to avoid multiple counts,
				# But each retweet is a new (though identical sentiment)...
				if 'quoted_status' in status.keys():
					self.append_list(self.process_status(status['quoted_status']))

		# Check out how by sending the status and modifying it in process_status
		# we are sending it by reference and so the local status is modified
		# So maybe could rewrite without returning anything
		self.append_list(self.process_status(status))

	def append_list(self, status):
		'''
		Make this a separate function to manage saving
		'''

		self.status_count += 1

		sys.stdout.write('Appending status: %d\r' % self.status_count)
		# Not entirely sure we need the flush...
		sys.stdout.flush()

		self.status_list.append(status)
		# print(len(self.status_list))

		if len(self.status_list) >= 10000:
			self.list_q.put(self.status_list, timeout = 1)

			# Create a new empty list
			# Note that modifying the object also modifies the queue object
			self.status_list = []

	def get_list(self, run_flag):
		'''
		Run a loop (in a thread) that pulls from the list queue
		'''

		while True:
			if not self.list_q.empty():
				# Queue.get() will block.  Might need to do a try, except
				# print(self.list_q.qsize())
				status_list = self.list_q.get()
				# print(len(status_list))
				self.save_list(status_list)

				# This call reduces the unfinished task count (built in to Queue)
				self.list_q.task_done()
			elif run_flag.is_set():
				time.sleep(0.01)
			else:
				print("List thread exiting")
				return True

		return False

	def save_list(self, status_list):
		'''
		Write new file at less regular intervals
		The files are named by the time
		'''

		sys.stdout.write('\n')
		sys.stdout.write('Saving pickle')#: %d\r' % self.status_count)
		sys.stdout.write('\n')
		sys.stdout.flush()

		# data_dir = self.data_dir

		date_dir = datetime.datetime.now().strftime('%Y_%m_%d')
		data_path = os.path.join(self.data_dir, date_dir)
		if not os.path.exists(data_path):
		    os.makedirs(data_path)

		file_name = datetime.datetime.now().strftime('%H_%M_%S')
		self.file_path = os.path.join(data_path, file_name) + '.p'

		# This is a cPickle (not a sea cucumber)
		with open(self.file_path, 'wb') as f:
			pickle.dump(status_list, f)
			# print('%s done saving' %file_name)

	def process_status(self, status):
		'''
		Modify each status dict so it containts desired data
		'''
		entities_hashtags = self.get_entities_hashtags(status)
		
		# The idea of having a seperate user database is attractive
		# It's not clear if user hashtags are useful after account is created
		# Also, there's a user defined location that may, or may not, be useful
		user_hashtags = self.get_user_hashtags(status)
		user_id = self.get_user_id(status)
		user_description = self.get_user_description(status)

		# Checked against entities hashtags, and its the same
		# text_hashtags = self.get_text_hashtags(status)
		
		# Add the new entries
		# Probably want to grab the user description too
		status['entities_hashtags'] = entities_hashtags
		status['user_id'] = user_id
		status['user_description'] = user_description
		status['user_hashtags'] = user_hashtags
		# status['text_hashtags'] = text_hashtags

		# Columns to keep
		keep_list = ['text', 'id', 'retweeted', 'coordinates', 'timestamp_ms',
					 'geo','lang','created_at', 'place', 'entities_hashtags',
					 'user_id', 'user_description', 'user_hashtags']
		# Delete the unwanted columns
		for key in status.keys():
			if key not in keep_list:
				del status[key]

		return status

	def get_user_id(self, status):
		'''
		Check to see if the user and description keys are there.
		Send the text to get hashtags
		'''
		if 'user' in status.keys():
			user_id = status['user']['id']
		else:
			user_id = np.nan

		return user_id

	def get_user_description(self, status):
		'''
		Check to see if the user and description keys are there.
		Send the text to get hashtags
		'''
		if 'user' in status.keys():
			user_description = status['user']['description']
		else:
			user_description = np.nan

		return user_description

	def get_entities_hashtags(self, status):
		'''
		Check to see if the entities, key is there.
		Place hashtags texts in a list
		'''
		if 'entities' in status.keys():
			entities_hashtags = [string.lower(tag['text']) for tag in status['entities']['hashtags']]
		else:
			entities_hashtags = []

		return entities_hashtags

	def get_user_hashtags(self, status):
		'''
		Check to see if the user and description keys are there.
		Send the text to get hashtags
		'''
		if 'user' in status.keys():
			# Check if value is None
			# Maybe combine conditions
			if status['user']['description']:
				user_hashtags = self.get_hashtags(status['user']['description'])
			else:
				user_hashtags = []
		else:
			user_hashtags = []

		return user_hashtags

	def get_text_hashtags(self, status):
		'''
		Check to see if the text keys is there.
		Send the text to get hashtags
		'''
		if 'text' in status.keys():
			# Check if value is None
			# Maybe combine conditions
			if status['text']:
				text_hashtags = self.get_hashtags(status['text'])
			else:
				text_hashtags = []
		else:
			text_hashtags = []

		return text_hashtags

	def get_hashtags(self, text):
		'''
		Collect user hashtags in the status text or user description
		Not sure if all hashtags sould be lowered, but probably
		'''
		hashtags = []
		# Check to see if the description is None
		for word in text.split(' '):
			if '#' in word:
				for hashtag in word.split('#')[1:]:
					if not word.isalnum():
				 		# Hyphens will be converted to spaces, which is ok
					 	hashtag_plus = re.sub(r'\W+', ' ', hashtag)
						hashtags.append(string.lower(hashtag_plus.split(' ')[0]))
					else:
						hashtags.append(string.lower(word))

		return hashtags


if __name__ == '__main__':

	# Both the data and queue processing classes take the same queue
	q = Queue(100)

	# A 147MB file collected with Trump and hillary as search terms
	# file_name = 'Aug20-1700.txt'
	file_name = 'Aug17Morning.txt'
	file_dir = 'Data'
	file_path = os.path.join(file_dir, file_name)
	pf = ProcessFile(q, file_path)
	pf.start_file_thread()


	pq = ProcessQueue(q)
	pq.start_line_thread()
	pq.start_list_thread()

	# Main thread blocks here until file is done
	pf.stop()

	# Once file is done tell processing it's ok to stop
	pq.stop()

	# data = pd.DataFrame(pq.status_list)
	# print(data.columns)
	# print(data[['entities_hashtags', 'user_hashtags']])
	# print(len(data))



