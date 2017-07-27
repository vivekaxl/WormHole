/**
	This script would submit a remote job using Livy and access the HPCC data.
	Note: The code is sent from HPCC (or ECLIDE) to Spark
*/
IMPORT python;
STRING run_command(STRING ip) := EMBED(python)
		import json, pprint, requests, textwrap, time
		# Note that sc -> spark context is already created by Livy
		spark_code = {
			'code': textwrap.dedent("""
			data = sc.textFile("file:///home/osboxes/GIT/mount_pnt2/thor/temp_store")
			parsedData = data.count()
			print parsedData
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