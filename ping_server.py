import ping, threading, time, socket, select, sys, struct
import binascii, collections, math, random, logging
import ping_reporter

log = ping_reporter.setup_log('PingServer')

class PingServer(threading.Thread):
	def __init__(self, d_addr, block_size=1024, timeout=2):
		self.timeout = timeout
		self.block_size = block_size # default; use setup for exact
		self.server = d_addr,socket.gethostbyname(d_addr)
		threading.Thread.__init__(self)
		self.debug = 0

		self.blocks = 0
		self.running = 1
		self.socket = ping.build_socket()
		self.empty_block = self.null_block()
		self.queued_events = collections.defaultdict(collections.deque)

	def setup_timeout(self, ID=0):
		Time = time.time()
		Times = struct.pack('d',Time)
		if ID == 0: ID = random.getrandbits(32) # ID size in bits

		ping.data_ping(self.socket,self.server[1],ID,Times)
		msg = ping.read_ping(self.socket,self.timeout)
		if not msg:                   raise Exception('PingServer::setup_timeout: no valid response from '+self.server[0])
		addr,rID,data = msg['address'],msg['ID'],msg['payload']
		log.debug("Addr=%s rID=%d Data=%d bytes"%(addr[0],rID,len(data)))
		if len(data) == 0:            raise Exception('PingServer::setup_timeout: null response from '+self.server[0])
		if rID != ID:                 raise Exception('PingServer::setup_timeout: invalid response id from '+self.server[0])
		if data != Times:             raise Exception('PingServer::setup_timeout: invalid response data from '+self.server[0])
		if addr[0] != self.server[1]: raise Exception('PingServer::setup_timeout: invalid response server from '+self.server[0])
		delay = time.time() - Time
		log.notice('echo delay: %.02fms'%(1000*delay))

	def setup_block(self, ID = 0):
		if ID == 0: ID = random.getrandbits(32) # ID size in bits
		Fill = chr(random.getrandbits(8)) # repeated data
		Filler = self.block_size * Fill

		ping.data_ping(self.socket,self.server[1],ID,Filler)
		msg = ping.read_ping(self.socket,self.timeout)
		if not msg:                   raise Exception('PingServer::setup_block: no valid response from '+self.server[0])
		addr,rID,data = msg['address'],msg['ID'],msg['payload']
		log.debug("Addr=%s rID=%d Data=%d bytes"%(addr[0],rID,len(data)))
		if len(data) == 0:            raise Exception('PingServer::setup_block: null response from '+self.server[0])
		if rID != ID:                 raise Exception('PingServer::setup_block: invalid response id from '+self.server[0])
		if data != len(data)*Fill:    raise Exception('PingServer::setup_block: invalid response data from '+self.server[0])
		if addr[0] != self.server[1]: raise Exception('PingServer::setup_block: invalid response server from '+self.server[0])
		self.block_size = len(data)
		self.empty_block = self.null_block()
		log.notice('echo length: %d bytes'%self.block_size)
		
	def setup(self):
		log.trace('PingServer::setup: testing server "%s"'%self.server[0])
		ID = random.getrandbits(32)
		self.setup_timeout(ID)
		self.setup_block(ID)

	def stop(self):
		log.info('PingServer terminating')
		self.running = 0

	def run(self):
		log.notice('PingServer starting')
		while self.running:
			start_blocks = self.blocks # updated asynchronously
			ready = select.select([self.socket], [], [], self.timeout)
			if ready[0] == []: # timeout
				if start_blocks != 0 and self.blocks != 0:
					log.error('%s timed out'%self.server[0])
				continue

			msg = ping.recv_ping(self.socket,self.timeout,True)
			if not msg: continue
			addr,block_id,data = msg['address'],msg['ID'],msg['payload']
			if block_id == 0:
				import binascii
				raise Exception('received packet w/ ID 0 packet: '+binascii.hexlify(msg['raw']))
			self.process_block(addr[0],block_id,data)

	def process_block(self, addr, ID, data):
		if ID == 0: raise Exception('server responded with ID 0 packet')

		while len(self.queued_events[ID]):
			handler,timer,args = self.queued_events[ID].popleft()
			if not timer.is_alive(): continue
			timer.cancel()
			
			if handler == self.write_block_timeout:
				if self.debug: log.trace('%s (block %d) updated'%(self.server[0],ID))
				data = args[1]
			if handler == self.read_block_timeout:
				if self.debug: log.trace('%s (block %d) read'%(self.server[0],ID))
				callback,cb_args = args[1],args[2]
				if len(data) > 0: callback(ID,data,*cb_args)
				else:             callback(ID,self.null_block(),*cb_args)
			if handler == self.delete_block_timeout:
				if self.debug: log.trace('%s (block %d) deleted'%(self.server[0],ID))
				data = ''

		if len(data) == 0:
			self.blocks = self.blocks - 1
		else:
			#print self.server[0],'(block %d)'%ID,'(%d):'%len(data),data
			ping.data_ping(self.socket, addr, ID, data)

	def null_block(self):
		return self.block_size * struct.pack('B',0)
		
	def async_timeout(self, func, args):
		t = threading.Timer(self.timeout,func,args)
		t.start()
		return t

	def event_insert(self, ID, handler, args):
		timer = self.async_timeout(handler,args)
		self.queued_events[ID].append((handler,timer,args))
		return timer

	# read / write / delete a single block
	def write_block(self, ID, data, blocking = False):
		# add a block to the queue (or delete if equivalent)
		log.trace('PingServer::write_block: ID=%d bytes=%d blocking=%s'%(ID,len(data),blocking))
		if ID == 0: raise Exception('write_block: invalid block ID (0)')
		if data == '%c'%0 * len(data): return self.delete_block(ID,blocking)
		t = self.event_insert(ID,self.write_block_timeout,[ID,data[:self.block_size]])
		if blocking: t.join()
		return t

	def delete_block(self, ID, blocking = False):
		log.trace('PingServer::delete_block: ID=%d blocking=%s'%(ID,blocking))
		if ID == 0: raise Exception('delete_block: invalid block ID (0)')
		t = self.event_insert(ID,self.delete_block_timeout,[ID])
		if blocking: t.join()
		return t

	def read_block(self, ID, callback, cb_args = [], blocking = False):
		log.trace('PingServer::read_block: ID=%d blocking=%s'%(ID,blocking))
		if ID == 0: raise Exception('read_block: invalid block ID (0)')
		t = self.event_insert(ID,self.read_block_timeout,[ID,callback,cb_args])
		if blocking: t.join()
		return t

	def read_block_timeout(self, ID, callback, cb_args):
		log.debug('PingServer::read_block_timeout: ID=%d callback=%s'%(ID,callback.__name__))
		callback(ID,self.null_block(),*cb_args)

	def delete_block_timeout(self, ID):
		log.debug('PingServer::delete_block_timeout: ID=%d'%ID)
		# do nothing; we're marked invalid anyhow
		pass

	def write_block_timeout(self, ID, data):
		log.debug('PingServer::write_block_timeout: ID=%d bytes=%d'%(ID,len(data)))
		self.blocks = self.blocks + 1
		# force update queue (as if packet arrived)
		if ID == 0: raise Exception('write_block_timeout: ID == 0')
		self.process_block(self.server[1], ID, data)

def print_block(ID, data):
	print '----- print block -----'
	print 'block',ID,'bytes',len(data)
	print data
	print '----- print block -----'

if __name__ == "__main__":
	ping_reporter.start_log(log,logging.TRACE)
	server = ping.select_server(1)

	from ping_reporter import humanize_bytes
	try:
		PS = PingServer(server)
		PS.debug = 1
		PS.setup()
		PS.start()
		print 'traffic:',ping.ping_count,'pings ('+humanize_bytes(ping.ping_bandwidth)+')'
		PS.read_block(2,print_block)
		time.sleep(4)
		PS.write_block(2,'coconut')
		time.sleep(3)
		print 'traffic:',ping.ping_count,'pings ('+humanize_bytes(ping.ping_bandwidth)+')'

		PS.write_block(1,'apples')
		PS.read_block(1,print_block)
		PS.delete_block(1)
		PS.read_block(1,print_block)
		time.sleep(4)
		print 'traffic:',ping.ping_count,'pings ('+humanize_bytes(ping.ping_bandwidth)+')'
		
		PS.write_block(1,'apples')
		time.sleep(2)
		PS.read_block(1,print_block)
		time.sleep(4)
		PS.read_block(1,print_block)
		time.sleep(1)
		PS.write_block(1,'bananas')
		time.sleep(1)
		PS.read_block(1,print_block)
		time.sleep(1)
		PS.read_block(1,print_block)
		PS.read_block(1,print_block)
		time.sleep(1)
		PS.delete_block(1)
		print 'traffic:',ping.ping_count,'pings ('+humanize_bytes(ping.ping_bandwidth)+')'
		while True:
			time.sleep(1)
		print 'terminate'
	except KeyboardInterrupt:
		print "Keyboard Interrupt"
	except Exception:
		print 'General Exception'
		from traceback import print_exc
		print_exc()
	finally:
		PS.stop()
		print 'traffic:',ping.ping_count,'pings ('+ping.humanize_bytes(ping.ping_bandwidth)+')'
		sys.exit(1)
		
