import socket
import pickle

def server():
	host = ''
	port = 20000
	s = socket.socket()
	s.bind((host, port))
	
	s.listen(1)
	c, addr = s.accept()
	
	collector = ""
	while True:
		data = c.recv(1024)
		if not data: break
		collector += data
	c.close()
	
	recv = pickle.loads(collector)
	print recv
	print len(recv)
	

if __name__ == "__main__":
	server()
		
