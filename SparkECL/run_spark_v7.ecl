/**
	This script would submit a remote job using Livy and access the HPCC data.
	Note: The code is sent from HPCC (or ECLIDE) to Spark
*/
IMPORT python;
IMPORT STD;
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

// since python  has strict indentation policy, the each line of the code is seperated by ';'. The other possible solution could pass a SET of STRING and modify it in the python side. 
string code := 
'from numpy import array\n'+
'from math import sqrt\n'+
'from pyspark.mllib.clustering import KMeans, KMeansModel\n'+
'def process_line(line):\n'+
'    t_line = line.strip()[1:-1]\n'+
'    t_line = t_line.split(\', \"__fileposition__\":\')[0]\n'+
'    tt_line = [t_l.split(\":\")[-1].strip().replace(\'\"\', \"\").replace(\"?\", \"1\") for t_l in t_line.split(\",\")]\n'+
'    values = map(float, tt_line)\n'+
'    return values\n'+
'data = sc.textFile(\"file:///home/osboxes/mount_point/thor/clustering_medium_kegg\")\n'+
'parsedData = data.map(lambda line: process_line(line))\n'+
'clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode=\"random\")\n'+
'def error(point):\n'+
'    center = clusters.centers[clusters.predict(point)]\n'+
'    return sqrt(sum([x**2 for x in (point - center)]))\n'+
'WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)\n'+
'print(\"Within Set Sum of Squared Error = \" + str(WSSSE))\n' ;
run_command('192.168.56.101:8998', code);