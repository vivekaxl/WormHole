/**
	This script would submit a remote job using Livy.
	Note:
		1. No data is being exchanged between HPCC and Spark
		2. The code resides on the Spark cluster rather than HPCC
*/
IMPORT python;
SET OF STRING run_command() := EMBED(python)
		import urllib2
		import json
		import time
		
		# file which contains the script to run
		d = '{"file": "file:/home/osboxes/spark-1.6.0/examples/src/main/python/pi.py"}'
		# ip of the spark cluster
		url = 'http://192.168.56.101:8998/batches'
		# curl request to submit spark job
		req = urllib2.Request(url, d, {'Content-Type': 'application/json'})
		f = urllib2.urlopen(req)
		output_string = f.readlines()
		f.close()
		# converting json string to dictionary 
		obj = json.loads(output_string[0]) 
		# Extract id
		id = obj['id']
		
		# Modifying url to start polling
		url += str(id)
		# Polling
		state = False
		while state is False:
				req = urllib2.Request(url)
				f = urllib2.urlopen(req)
				output_string = f.readlines()
				f.close()
				
				obj = json.loads(output_string[0])
				if obj['state'] == 'success': state = True
				else: time.sleep(1)
		# obj['log'] is a unicode object which need to be converted to str
		return map(str, obj['log'])
ENDEMBED;   

run_command();