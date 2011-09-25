import ping, threading, time, socket, select, sys, struct, Queue
import binascii, collections, math, random, logging
import ping_reporter

log = ping_reporter.setup_log('PingServer')

class PingTimer(threading.Thread): # helper class for PingServer to manage timeouts
	def __init__(self, event):
		self.queue = Queue.PriorityQueue()
		threading.Thread.__init__(self)
		self.running = False
		self.event = event

	def run(self):
		self.running = True
		log.debug('PingTimeout starting')
		while self.running:
			timeout = None
			self.event.clear()
			if self.queue.qsize() > 0:
				timeout = self.process()
			self.event.wait(timeout)

	def stop(self):
		log.debug('PingTimeout terminating')
		self.running = False
		self.event.set()

	def process(self):
		while self.queue.qsize() > 0:
			try:                expire,event,callback,cb_args = item = self.queue.get_nowait()
			except Queue.Empty: break # our qsize check isn't guaranteed to prevent this
			if event.is_set():  continue # event was completed; ignore it
			if expire > time.time():
				self.queue.put(item)
				return expire - time.time()
			callback(*cb_args)
			event.set() # make sure no one executes it
		return None

	def add_callback(self, timeout, handler, args):
		event = threading.Event()
		item = (time.time()+timeout,event,handler,args)
		self.queue.put(item)
		self.event.set()
		return event


class PingServer(threading.Thread):
	def __init__(self, d_addr, block_size=1024, initial_timeout=2):
		self.block_size = block_size # default; use setup for exact
		self.server = d_addr,socket.gethostbyname(d_addr)
		self.running_timeout = initial_timeout
		threading.Thread.__init__(self)
		self.listeners = []
		self.debug = 0

		# timeout events are queued and executed in a seperate thread
		self.timer_event = threading.Event()
		self.timer = PingTimer(self.timer_event)


		self.blocks = 0
		self.running = False
		self.socket = ping.build_socket()
		self.empty_block = self.null_block()
		self.queued_events = collections.defaultdict(collections.deque)
	
	def timeout(self):		return 2.0/5.0 # self.running_timeout
	def safe_timeout(self): return 3 * self.timeout()

	def setup_timeout(self, ID=0):
		Time = time.time()
		Times = struct.pack('d',Time)
		if ID == 0: ID = random.getrandbits(32) # ID size in bits

		ping.data_ping(self.socket,self.server[1],ID,Times)
		msg = ping.read_ping(self.socket,self.timeout())
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
		msg = ping.read_ping(self.socket,self.timeout())
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
		self.running = False
		log.info('PingServer terminating')
		self.timer.stop()

	def run(self):
		self.running = True
		log.notice('PingServer starting')
		self.timer.start()
		while self.running:
			start_blocks = self.blocks # updated asynchronously
			ready = select.select([self.socket], [], [], self.timeout())
			if ready[0] == []: # timeout
				if start_blocks != 0 and self.blocks != 0:
					log.error('%s timed out'%self.server[0])
			try:
				msg = ping.recv_ping(self.socket,self.timeout())
				if not msg: continue
			except:
				continue
			addr,block_id,data = msg['address'],msg['ID'],msg['payload']
			if block_id == 0:
				import binascii
				raise Exception('received packet w/ ID 0 packet: '+binascii.hexlify(msg['raw']))
			self.process_block(addr[0],block_id,data)

	def process_block(self, addr, ID, data):
		if ID == 0: raise Exception('server responded with ID 0 packet')

		while len(self.queued_events[ID]):
			handler,event,args = self.queued_events[ID].popleft()
			if event.is_set(): continue
			event.set()
			
			if handler == self.write_block_timeout:
				if self.debug: log.trace('%s (block %d) updated'%(self.server[0],ID))
				data = args[1]
			elif handler == self.read_block_timeout:
				if self.debug: log.trace('%s (block %d) read'%(self.server[0],ID))
				callback,cb_args = args[1],args[2]
				if len(data) > 0: callback(ID,data,*cb_args)
				else:             callback(ID,self.null_block(),*cb_args)
			elif handler == self.delete_block_timeout:
				if self.debug: log.trace('%s (block %d) deleted'%(self.server[0],ID))
				data = ''

		if len(data) == 0:
			self.blocks = self.blocks - 1
		else:
			if len(self.listeners): self.process_listeners(addr, ID, data)
			log.trace('%s: sending %d bytes from block %d'%(self.server[0],len(data),ID))
			ping.data_ping(self.socket, addr, ID, data)

	def process_listeners(self, addr, ID, data):
		if not self.listeners: raise Exception('process_listeners invoked without valid listeners on ID=%d'%ID)
		self.listeners = [l for l in self.listeners if l[0] >= time.time()] # clean the listeners
		for x in self.listeners:
			expire,handler,cb_args = x
			handler(ID, addr, data, *cb_args)

	def add_listener(self, handler, timeout, args):
		log.debug('add_listener: timeout=%d handler=%s'%(timeout,handler))
		expire = time.time() + timeout
		self.listeners.append((expire,handler,args))

	def null_block(self):
		return self.block_size * struct.pack('B',0)
		
	def event_insert(self, ID, handler, args):
		event = self.timer.add_callback(self.timeout(), handler, args)
		self.queued_events[ID].append((handler,event,args))
		return event

	# read / write / delete a single block
	def write_block(self, ID, data, blocking = False):
		# add a block to the queue (or delete if equivalent)
		log.trace('PingServer::write_block: ID=%d bytes=%d blocking=%s'%(ID,len(data),blocking))
		if ID == 0: raise Exception('write_block: invalid block ID (0)')
		if data == '%c'%0 * len(data): return self.delete_block(ID,blocking)
		event = self.event_insert(ID,self.write_block_timeout,[ID,data[:self.block_size]])
		if blocking: event.wait()
		return event

	def delete_block(self, ID, blocking = False):
		log.trace('PingServer::delete_block: ID=%d blocking=%s'%(ID,blocking))
		if ID == 0: raise Exception('delete_block: invalid block ID (0)')
		t = self.event_insert(ID,self.delete_block_timeout,[ID])
		if blocking: t.wait()
		return t

	def read_block(self, ID, callback, cb_args = [], blocking = False):
		log.trace('PingServer::read_block: ID=%d blocking=%s'%(ID,blocking))
		if ID == 0: raise Exception('read_block: invalid block ID (0)')
		t = self.event_insert(ID,self.read_block_timeout,[ID,callback,cb_args])
		if blocking: t.wait()
		return t

	def read_block_timeout(self, ID, callback, cb_args):
		log.debug('PingServer::read_block_timeout: ID=%d callback=%s'%(ID,callback.__name__))
		callback(ID,self.null_block(),*cb_args)
		print 'read_timeout',ID

	def delete_block_timeout(self, ID):
		log.debug('PingServer::delete_block_timeout: ID=%d'%ID)
		# do nothing; we're marked invalid anyhow
		pass

	def write_block_timeout(self, ID, data):
		log.trace('PingServer::write_block_timeout: ID=%d bytes=%d'%(ID,len(data)))
		self.blocks = self.blocks + 1
		# force update queue (as if packet arrived)
		if ID == 0: raise Exception('write_block_timeout: ID == 0')
		self.process_block(self.server[1], ID, data)

def print_block(ID, data):
	print '----- print block -----'
	print 'block',ID,'bytes',len(data)
	print data
	print '----- print block -----'

def __live_blocks(ID, addr, data, datastore):
	datastore[ID] = 1

def live_blocks(PServer, timeout=None):
	store = {}
	if not timeout: timeout = PServer.safe_timeout()
	PServer.add_listener(__live_blocks,timeout,[store])
	time.sleep(timeout)
	return store
		
def used_blocks(blocks):
	result,lookup = {},{}
	for x in blocks:
		if x-1 in lookup:
			lookup[x] = lookup[x-1]
			result[lookup[x]] += 1
		else:
			lookup[x] = x
			result[x] = 1
	return result


def free_blocks(blocks):
	result = {}
	if 1 not in blocks:
		if not blocks:         result[1] = 0
		elif len(blocks) == 0: result[1] = 0
		else:                  result[1] = min(blocks.keys())-1
	for x in blocks:
		if not x+1 in blocks: result[x+1] = 0
		if not x-1 in blocks:
			if not len(result): continue
			block = max(result.keys())
			result[block] = x-block
	return result

if __name__ == "__main__":
	ping_reporter.start_log(log,logging.DEBUG)
	server = ping.select_server(log,1)

	from ping_reporter import humanize_bytes
	try:
		PS = PingServer(server)
		PS.debug = 1
		PS.setup()
		PS.start()
		print 'traffic:',ping.ping_count,'pings ('+humanize_bytes(ping.ping_bandwidth)+')'
		PS.read_block(2,print_block)
		time.sleep(2)
		PS.write_block(2,'coconut')
		time.sleep(1)
		print 'traffic:',ping.ping_count,'pings ('+humanize_bytes(ping.ping_bandwidth)+')'

		PS.write_block(1,'apples')
		PS.read_block(1,print_block)
		PS.read_block(1,print_block)
		time.sleep(2)
		print 'traffic:',ping.ping_count,'pings ('+humanize_bytes(ping.ping_bandwidth)+')'
	
		log.info('testing block metrics')
		blocks = live_blocks(PS)
		log.debug('blocks: %s'%blocks)
		log.debug('--used: %s'%used_blocks(blocks))
		log.debug('--free: %s'%free_blocks(blocks))
		
		PS.delete_block(1)
		time.sleep(2)
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
		print 'traffic:',ping.ping_count,'pings ('+humanize_bytes(ping.ping_bandwidth)+')'
		sys.exit(1)
		
