/**
	This script would submit a remote job using Livy.
	Note:
		1. No data is being exchanged between HPCC and Spark
		2. The code is sent from HPCC to Spark
*/
IMPORT python;
STRING run_command(STRING ip) := EMBED(python)
		import json, pprint, requests, textwrap, time
		# Note that sc -> spark context is already created by Livy
		spark_code = {
			'code': textwrap.dedent("""
			import random
			NUM_SAMPLES = 100000
			def sample(p):
				x, y = random.random(), random.random()
				return 1 if x*x + y*y < 1 else 0
			count = sc.parallelize(xrange(0, NUM_SAMPLES)).map(sample).reduce(lambda a, b: a + b)
			print "Pi is roughly %f" % (4.0 * count / NUM_SAMPLES)
				""")
		}
		
		# Code to start a session to execute pyspark commands
		host = 'http://' + ip
		r = requests.post(host + '/sessions', data=json.dumps({'kind': 'pyspark'}), headers={'Content-Type': 'application/json'})
		session_url = host + r.headers['Location']
		
		# Polling to check if the session is ready		
		while True:
			time.sleep(2)
			response = requests.get(session_url, headers={'Content-Type': 'application/json'}).json()
			if str(response['state']) == 'idle': break
		
		# Submitting the pyspark code 
		statements_url = session_url + '/statements'
		r = requests.post(statements_url, data=json.dumps(spark_code), headers={'Content-Type': 'application/json'})

		# Polling to check if the session is ready
		while True:
			time.sleep(2)
			response = requests.get(session_url, headers={'Content-Type': 'application/json'}).json()
			if str(response['state']) == 'idle': break
		
		# Retriving results
		response = requests.get(statements_url, headers={'Content-Type': 'application/json'}).json()
		
		# Stopping the session created
		requests.delete(session_url, headers={'Content-Type': 'application/json'})
		
		return  str(response['statements'][0]['output']['data']['text/plain'])
ENDEMBED;   

run_command('192.168.56.101:8998');