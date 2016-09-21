# Tweepy-Tools.py

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweets import ProcessQueue
from threading import Thread
from ConfigParser import ConfigParser
from Queue import Queue
import json
import time
import signal
import sys

class TwitterStream():
	'''
	This class contains functions to setup and start streaming twitter data
	'''

	def __init__(self, track_list, data_dir, config_file  = 'twitter-keys-file.ini'):
		'''
		Defualt startup
		'''

		self.config_file = config_file

		self.track_list = track_list

		# Pass the queue to the stream.
		# This starts a listener that dumps to the queue
		q = Queue(100)
		# Don't have to return here...
		self.stream = self.get_stream(q)

		# Get an instance of the queue processor
		# A little awkward just passing data_dir arg without using
		self.pq = ProcessQueue(q, data_dir)


	def start_stream(self):
		'''
		Start the stream and the processing
		'''

		# Setting async to True means that we can kill the loop using stream.disonnect()
		self.stream.filter(languages = ['en'], track = self.track_list, async = True)

		# Change this to be one fucntion
		self.pq.start_line_thread()
		self.pq.start_list_thread()

	def stop_stream(self, sig_int, frame):
		'''
		Call this to stop the stream and wrap up the queues
		'''

		self.stream.disconnect()
		# Once streaming is done tell processing it's ok to stop
		self.pq.stop()

	def get_auth(self, account = 'DEFAULT'):
		'''
		Set up the auth using the account in the config file
		By default the DEFAULT account is used
		'''

		config = ConfigParser()
		config.read(self.config_file)
		config.sections()

		# Apparently the python 3 interface to configparser has changed quite a bit
		consumer_key = config.get(account, 'consumer_key')
		consumer_secret = config.get(account, 'consumer_secret')
		access_token = config.get(account, 'access_token_key')
		access_token_secret = config.get(account, 'access_token_secret')

		auth = OAuthHandler(consumer_key, consumer_secret)
		auth.set_access_token(access_token, access_token_secret)

		return auth

	def get_stream(self, queue):
		'''
		This handles the connection to Twitter Streaming API
		Takes a queue used by on_data
		'''

		# file_name = 'test.txt'
		auth = self.get_auth()
		listener = StdOutListener(queue)
		stream = Stream(auth, listener)

		return stream


class StdOutListener(StreamListener):
	'''
	This is a basic listener that just prints received tweets to stdout.
	Inherits from StreamListener, which includes on_data and on_error stubs
	Actually it appears that on_data sends status objects to on_status
	StdOutListener inherits from StreamListener, which has stubs:
	on_data() and on_error() that get called upon reciept of
	'''

	def __init__(self, data_queue):
		'''
		The super function allows assessment of parent class init, while 
		still having a child init.
		The class needs an open file handle to write to
		'''
		super(StdOutListener, self).__init__()
		# self.file_name = file_name
		self.q = data_queue
		self.count = 0


	def view_stream(self, data):
		'''
		Look at the data on the fly
		'''
		# Convert to a dict
		status = json.loads(data)
		try:
			if status['place'] != None:
				print(status['place'])
		except:
			pass
			# print('Place key missing')
		
		try:
			if status['coordinates'] != None:
				print(status['coordinates'])
		except:
			pass
			# print('Coordinates key missing')


		try:
			if status['entities'] != None:
				print(status['entities']['hashtags']['text'])
		except:
			pass
			# print('Hashtags missing')
			
	# def on_status(self, status):
	# 	'''
	# 	Instead of raw data, on_status is called with a status
	# 	object.  It appears to be identical to the dict created
	# 	using JSON load, but is an object... with attributes, not keys
	# 	'''

	# 	print(dir(status))

	# 	return True

	def on_data(self, data):
		'''
		Just write all the returns to a file and look at it right quick
		So, for some reason the first example I saw used on_data...
		on_status converts it for you... no big
		Really should import another class or something
		'''
		# try:
		# self.count += 1
		# except:
			# self.count = 0

		# print(self.count)
		self.q.put(data)
		# file_name = 'Aug20-1700.txt'
		# with open(self.file_name, 'a') as outfile:
			# outfile.write(data)
		return True

	# def on_error(self, err_status):
	# 	print(err_status)

	def on_error(self, status):
		print('Error on status', status)

	def on_limit(self, status):
		print('Limit threshold exceeded', status)

	def on_timeout(self, status):
		print('Stream disconnected; continuing...')


if __name__ == '__main__':


	# The filter modifies the stream capture to include tweets that fit the
	# search criteria logical combined with logical or.  For this reason

	# Define the save directory first
	data_dir = sys.argv[1]

	# Bring the track_list in from a pickle
	track_path = sys.argv[2]

	with open(track_path, 'rb') as track_file:
		track_list = pickle.load(track_file)

	print(track_list)

	# Initiate with credentials.
	ts = TwitterStream(track_list, data_dir)

	# The main thread exits, which feels awkward.
	# Maybe block or loop on something in main thread.
	signal.signal(signal.SIGINT, ts.stop_stream)
	ts.start_stream()



	# A stream sample gets 1% (?) of tweets.  It doesn't have an async mode, so run in a thread.
	# stream.sample()










