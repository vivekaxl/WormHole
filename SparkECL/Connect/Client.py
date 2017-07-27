import socket
import random
import pickle
import string

def client():
	host = '192.168.56.101'
	port = 20000
	
	s = socket.socket()
	s.connect((host, port))
	
	content = ''.join(random.choice(string.ascii_uppercase) for _ in xrange(500))
	
	payload = pickle.dumps(content)
	s.send(payload)
	s.close()
	print content, len(content)
	
if __name__ == "__main__":
	client()