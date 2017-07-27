/**
	This script would submit a remote job using Livy and access the HPCC data.
	Note: The code is sent from HPCC (or ECLIDE) to Spark
*/
IMPORT python;
STRING run_command(STRING ip, STRING code) := EMBED(python)
		import json, pprint, requests, textwrap, time
		# Note that sc -> spark context is already created by Livy
		# Enclosing the code (passed as parameter) with Livy structure
		code = 'spark_code = {\n \'code\': textwrap.dedent(\"\"\"' + code + '\"\"\")}'
		# Executing the python code. This is native to python
		exec(code)
		
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


rs := {integer id, integer val};
ds1 := dataset([{1,1}], rs);

rs trans1(integer id, rs l) := TRANSFORM
	SELF.id := id;
	SELF.val := random();
END;
ds2 := normalize(ds1, 500, trans1(COUNTER, LEFT));
output(ds2,, '~thor::temp_store', OVERWRITE);

// since python  has strict indentation policy, the each line of the code is seperated by ';'. The other possible solution could pass a SET of STRING and modify it in the python side. 
string code := 'data = sc.textFile("file:///home/osboxes/GIT/mount_pnt2/thor/temp_storeNEW"); parsedData = data.count(); print parsedData';
run_command('192.168.56.101:8998', code);