import os,sys,socket,struct,select,time,binascii

ping_count = 0
ping_bandwidth = 0

def carry_add(a, b):
	c = a + b
	return (c & 0xFFFF) + (c >> 16)

def checksum(msg):
	s = 0
	if len(msg)%2: # pad with NULL
		msg = msg + '%c'%0
	for i in range(0, len(msg)/2*2, 2):
		w = ord(msg[i]) + (ord(msg[i+1]) << 8)
		s = carry_add(s, w)
	return ~s & 0xFFFF

def build_ping(ID, data):
	if ID == 0: raise Exception('Invalid BlockID (0): many servers will corrupt ID=0 ICMP messages')

	data = str(data) # string type, like the packed result

	# Header is type (8), code (8), checksum (16), id (16), sequence (16)
	icmp_type		= 8 # ICMP_ECHO_REQUEST
	icmp_code		= 0 # Can be anything, but reply MUST be 0
	icmp_checksum		= 0 # 0 for initial checksum calculation
#	icmp_id			= (ID >> 16) & 0xFFFF
#	icmp_sequence		= (ID <<  0) & 0xFFFF
	block_id		= ID # append id & seq for 4-byte identifier

	header = struct.pack("bbHL", icmp_type, icmp_code, icmp_checksum, block_id)
	icmp_checksum = checksum(header+data)
	header = struct.pack("bbHL", icmp_type, icmp_code, icmp_checksum, block_id)

	# Return built ICMP message
	return header+data

def build_socket():
	icmp = socket.getprotobyname("icmp")
	try:
		icmp_socket = socket.socket(socket.AF_INET, socket.SOCK_RAW, icmp)
	except socket.error, (errno, msg):
		if errno == 1: # Operation not permitted
			msg = msg + (" (ICMP messages can only be sent from processes running as root)")
			raise socket.error(msg)
		raise # raise the original error
	return icmp_socket

def time_ping(d_socket, d_addr, ID=1):
	data = struct.pack("d",time.time())
	return data_ping(d_socket, d_addr, ID, data)

def data_ping(d_socket, d_addr, ID, data):
	global ping_count, ping_bandwidth
	packet = build_ping(ID,data)
	d_addr  =  socket.gethostbyname(d_addr)
	d_socket.sendto(packet, (d_addr, 1))
	if 1:
		ping_count = ping_count + 1
		ping_bandwidth = ping_bandwidth + len(packet)

def recv_ping(d_socket, timeout):
	d_socket.settimeout(timeout)
	data,addr = d_socket.recvfrom(65536)
	if len(data) < 28: return addr,0,''
	type, code, checksum, block_id = struct.unpack("bbHL", data[20:28])
	return addr,block_id,data[28:]

def receive_ping(my_socket, ID, timeout):
        timeLeft = timeout
        while True:
                startedSelect = time.time()
                whatReady = select.select([my_socket], [], [], timeLeft)
		print 'selecting...'
                if whatReady[0] == []: # Timeout
                        return
        
                timeReceived = time.time()
                howLongInSelect = (timeReceived - startedSelect)
                recPacket, addr = my_socket.recvfrom(1024)
                icmpHeader = recPacket[20:28]
                type, code, checksum, packetID = struct.unpack("bbHL", icmpHeader)
                if packetID == ID:
                        bytesInDouble = struct.calcsize("d")
                        timeSent = struct.unpack("d", recPacket[28:28 + bytesInDouble])[0]
                        return timeReceived - timeSent
        
                timeLeft = timeLeft - howLongInSelect
                if timeLeft <= 0:
                        return

def single_ping(dest_addr, timeout):
	my_socket = build_socket()
	my_ID = os.getpid() & 0xFFFF

	print 'sending...'
	time_ping(my_socket, dest_addr, my_ID)
	delay = receive_ping(my_socket, my_ID, timeout)
	my_socket.close()
	return delay

def verbose_ping(dest_addr, timeout = 2, count = 4):
	for i in xrange(count):
		print "ping %s..." % dest_addr,
		try:
			delay  =  single_ping(dest_addr, timeout)
		except socket.gaierror, e:
			print "failed. (socket error: '%s')" % e[1]
			break

		if delay  ==  None:
			print "failed. (timeout within %ssec.)" % timeout
		else:
			delay  =  delay * 1000
			print "get ping in %0.4fms" % delay
	print

if __name__ == '__main__':
	if 0:
		s = build_socket()
		print 'starting...'
		for x in range(1,1000):
			data_ping(s,"172.16.1.1",x,struct.pack('d',x))
			print 'ping cycled...'
			recv_ping(s,1)
		print '1000 pings sent'
	#verbose_ping("172.16.1.1",2,1000)
	verbose_ping("google.com")
	#verbose_ping("192.168.1.1")

