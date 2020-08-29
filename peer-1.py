#Nakas Konstantinos-Panagiotis 	cse32501@cs.uoi.gr	2501 
#Sunergaths:Katsantas Thomas

	
# Event-driven code that behaves as either a client or a server
# depending on the argument.  When acting as client, it connects 
# to a server and periodically sends an update message to it.  
# Each update is acked by the server.  When acting as server, it
# periodically accepts messages from connected clients.  Each
# message is followed by an acknowledgment.
#
# Tested with Python 2.7.8 and Twisted 14.0.2
#
import optparse
from random import randint
from twisted.internet.protocol import Protocol, ClientFactory
from twisted.internet import reactor
import time
import sys

Update_Peer = list()
transmission = 0
file_report = 0
timeout_participant1 = [0,0,0,0,0]
timeout_participant2 = [0,0,0,0,0]

def parse_args():
	usage = """usage: %prog [options] [client|server] [hostname]:port

	python peer.py server 127.0.0.1:port """

	parser = optparse.OptionParser(usage)

	_, args = parser.parse_args()

	peertype, addresses = args

	def parse_address(addr):
		if ',' in addr:
			addr1,addr2 = addr.split(',',1)
			host,port = addr1.split(':', 1)
			host2,port2 = addr2.split(':', 1)
			return host, int(port),host2,int(port2)
		else:
			host, port = addr.split(':', 1)
			return host, int(port)

		if not port.isdigit():
			parser.error('Ports must be integers.')

		

	return peertype, parse_address(addresses)


class Peer(Protocol):
	acks = 0
	sended_acks = 0
	connected = False
	votes_number = list()

	def __init__(self, factory, peer_type):
		global timeout_participants
		self.pt = peer_type
		self.factory = factory

		for i in range(5):
			self.votes_number.append(0)

	def timeout_Coordinator(self, msg_num):
		global Update_Peer
		if self.votes_number[msg_num-1] < 2:
			self.votes_number[msg_num-1] = 2
			file_report.write("GLOBAL_ABORT " + str(msg_num) +  " due to timeout from coordinator\n")
			print "Sending GLOBAL_ABORT " + str(msg_num) + " due to timeout from coordinator\n"
			for i in Update_Peer:
				try:
					i.transport.write('GLOBAL_ABORT '+str(msg_num)+"\n")
				except Exception, ex1:
					print "Exception trying to send: ", ex1.args[0]
	
			

	def timeout_Participant(self, msg_num):
		global timeout_participant1, timeout_participant2
		if int(timeout_participant1[msg_num]) == 0 and self.pt == '1':
			file_report.write("VOTE_ABORT " + str(msg_num) + " due to timeout from participant 1\n")
			print "Sending VOTE_ABORT " + str(msg_num) + " due to timeout from participant 1"
			Update_Peer[0].transport.write('VOTE_ABORT'+str(msg_num)+"\n")
		if int(timeout_participant2[msg_num]) == 0 and self.pt == '2':
			file_report.write("VOTE_ABORT " + str(msg_num) + " due to timeout from participant 2\n")
			print "Sending VOTE_ABORT " + str(msg_num) + " due to timeout from participant 2"
			Update_Peer[0].transport.write('VOTE_ABORT'+str(msg_num)+"\n")
			

	def connectionMade(self):
		global transmission, Update_Peer,file_report
		print "Connected from", self.transport
		sys.stdout.flush()
		Update_Peer.append(self)
		if len(Update_Peer) ==  2:
			print("First peer "+str(self.pt))
			if (self.pt == '0'):
				transmission = transmission + 1
				file_report.write("START_2PC\n")
				reactor.callLater(3, self.sendUpdate,"",transmission)
				reactor.callLater(6, self.timeout_Coordinator,transmission)
			else:
				file_report.write("INIT\n")
		try:
			self.transport.write('<connection up>')
			self.connected = True
		except Exception, e:
			print e.args[0]
		self.ts = time.time()

	def sendUpdate(self,request,msg_num):
		global transmission, Update_Peer
		if self.pt == '0':
			print "Sending vote request " + str(transmission) + " to all participants"
			for i in Update_Peer:
				try:
					i.transport.write('VOTE_REQUEST '+str(transmission))
				except Exception, ex1:
					print "Exception trying to send: ", ex1.args[0]
			if self.connected == True and transmission<5:
				transmission = transmission + 1
				reactor.callLater(3, self.sendUpdate,"",transmission)
				reactor.callLater(6, self.timeout_Coordinator,transmission)
				
		else:
			print "Sending " + request + " to coordinator"
			if request == "COMMIT":
				Update_Peer[0].transport.write('VOTE_COMMIT' + str(msg_num) + 'from ' + self.pt + '\n')
				if int(msg_num) < 5:
					reactor.callLater(3, self.timeout_Participant,int(msg_num)-1)
			else:
				if int(msg_num) < 5:
					reactor.callLater(3, self.timeout_Participant,int(msg_num)-1)
				Update_Peer[0].transport.write('VOTE_ABORT' + str(msg_num) + 'from ' + self.pt + '\n')

	def sendAck(self,msg_num):
		self.ts = time.time()
		try:
			self.sended_acks += 1
			print "sendAck " + str(self.sended_acks) + " for message " + str(msg_num) + "\n"
			Update_Peer[0].transport.write("Ack"+ str(msg_num)+" from partcipant "+str(self.pt)+"\n")
		except Exception, e:
			print e.args[0]


	def dataReceived(self, data):
		global Update_Peer,file_report,transmission, timeout_participant1, timeout_participant2
		if ("<connect" not in data):		
			if self.pt == '0':
				if ("VOTE_COMMIT" in data):
					msg = int(data.split("COMMIT")[1].split("from")[0])
					self.votes_number[msg-1] = self.votes_number[msg-1] + 1
					if (self.votes_number[msg-1] == 2):
						file_report.write("GLOBAL_COMMIT " + data.split("COMMIT")[1].split("from")[0] + "\n")
						print "Sending GLOBAL_COMMIT " + data.split("COMMIT")[1].split("from")[0] + " to all participants"
						for i in Update_Peer:
							try:
								i.transport.write('GLOBAL_COMMIT '+data.split("COMMIT")[1].split("from")[0])
							except Exception, ex1:
								print "Exception trying to send: ", ex1.args[0]
				elif ("VOTE_ABORT" in data):
					msg = int(data.split("ABORT")[1].split("from")[0])
					if self.votes_number[msg-1] < 2:	
						self.votes_number[msg-1] = 2
						file_report.write("GLOBAL_ABORT " + data.split("ABORT")[1].split("from")[0] + "\n")
						print "Sending GLOBAL_ABORT " + data.split("ABORT")[1].split("from")[0] + " to all participants"
						for i in Update_Peer:
							try:
								i.transport.write('GLOBAL_ABORT '+data.split("ABORT")[1].split("from")[0])
							except Exception, ex1:
								print "Exception trying to send: ", ex1.args[0]
				if "Ack" in data:
					print(data+"\n")
			
			else:
				if "VOTE_REQUEST" in data:
					msg = int(data.split(" ")[1])
					if randint(0,9) <= 4:
						file_report.write("VOTE_COMMIT" + str(data.split(" ")[1]) + "\n")
						self.sendUpdate("COMMIT",data.split(" ")[1])
					else:						
						file_report.write("VOTE_ABORT" + str(data.split(" ")[1]) + "\n")
						self.sendUpdate("ABORT",data.split(" ")[1])
				elif "GLOBAL_COMMIT" in data:
					msg = int(data.split("GLOBAL_COMMIT ")[1])
					if self.pt == '1':
						timeout_participant1[msg-1] = timeout_participant1[msg-1] + 1
					else:
						timeout_participant2[msg-1] = timeout_participant1[msg-1] + 1
					file_report.write(data + "\n\n")
					self.sendAck(msg)
				elif "GLOBAL_ABORT" in data:
					msg = int(data.split("GLOBAL_ABORT ")[1])
					if self.pt == '1':
						timeout_participant1[msg-1] = timeout_participant1[msg-1] + 1
					else:
						timeout_participant2[msg-1] = timeout_participant1[msg-1] + 1
					file_report.write(data + "\n\n")
					self.sendAck(msg)

	def connectionLost(self, reason):
		print "Disconnected"
		self.connected = False
		self.done()

	def done(self):
		self.factory.finished(self.acks)


class PeerFactory(ClientFactory):

	def __init__(self, peertype, fname):
		print '@__init__'
		self.pt = peertype
		self.acks = 0
		self.fname = fname
		self.records = []

	def finished(self, arg):
		self.acks = arg
		self.report()

	def report(self):
		print 'Received '+str(self.acks)+' acks'

	def clientConnectionFailed(self, connector, reason):
		print 'Failed to connect to:', connector.getDestination()
		self.finished(0)

	def clientConnectionLost(self, connector, reason):
		print 'Lost connection.  Reason:', reason

	def startFactory(self):
		print "@startFactory"
		if self.fname != '':
			self.fp = open(self.fname, 'w+')

	def stopFactory(self):
		print "@stopFactory"
		if self.fname != '':
			self.fp.close()

	def buildProtocol(self, addr):
		print "@buildProtocol"
		protocol = Peer(self, self.pt)
		return protocol


if __name__ == '__main__':
	peer_type, address = parse_args()

	file_report = open("log-"+str(peer_type)+".txt",'w')

	if peer_type == '0':
		factory = PeerFactory('0', 'log')
		reactor.listenTCP(2501, factory)
		print "Starting node 0 @" + '127.0.0.1' + " port " + str('2501')
	elif peer_type == '1':
		factory = PeerFactory('1', 'log')
		reactor.listenTCP(2502, factory)
		print "Starting node 1 @" + '127.0.0.1' + " port " + str('2502')

		factory = PeerFactory('1', '')
		host, port = address
		print "Connecting to host " + host + " port " + str(port)
		reactor.connectTCP(host, port, factory)
	else:
		factory = PeerFactory('2', 'log')
		reactor.listenTCP(2503, factory)
		print "Starting node 2 @" + '127.0.0.1' + " port " + str('2503')

		factory = PeerFactory('2', '')
		host, port, host2, port2 = address
		print "Connecting to host " + host + " port " + str(port)
		reactor.connectTCP(host, port, factory)

		factory = PeerFactory('2', '')
		
		print "Connecting to host " + host2 + " port " + str(port2)
		reactor.connectTCP(host2, port2, factory)

	reactor.run()
