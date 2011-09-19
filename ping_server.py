import ping, threading, time, socket, select, sys, struct
import binascii, threading, collections, math, random


# delete -> write -> read 

class PingServer(threading.Thread):
	def __init__(self, d_addr, block_size=1024, timeout=2):
		self.timeout = timeout
		self.block_size = block_size # default; use setup for exact
		self.server = d_addr,socket.gethostbyname(d_addr)
		threading.Thread.__init__(self)

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
		addr,rID,data = ping.recv_ping(self.socket,self.timeout)
		print "Addr:",addr[0],"rID:",rID,"Data:",len(data),'bytes'
		if len(data) == 0:            raise Exception('PingServer::setup_timeout: null response from '+self.server[0])
		if rID != ID:                 raise Exception('PingServer::setup_timeout: invalid response id from '+self.server[0])
		if data != Times:             raise Exception('PingServer::setup_timeout: invalid response data from '+self.server[0])
		if addr[0] != self.server[1]: raise Exception('PingServer::setup_timeout: invalid response server from '+self.server[0])
		delay = time.time() - Time
		print "reply time:",int(1000*delay),"ms"

	def setup_block(self, ID = 0):
		if ID == 0: ID = random.getrandbits(32) # ID size in bits
		Fill = chr(random.getrandbits(8)) # repeated data
		Filler = self.block_size * Fill

		ping.data_ping(self.socket,self.server[1],ID,Filler)
		addr,rID,data = ping.recv_ping(self.socket,self.timeout)
		print "Addr:",addr[0],"rID:",rID,"Data:",len(data),"bytes"
		if len(data) == 0:            raise Exception('PingServer::setup_block: null response from '+self.server[0])
		if rID != ID:                 raise Exception('PingServer::setup_block: invalid response id from '+self.server[0])
		if data != len(data)*Fill:    raise Exception('PingServer::setup_block: invalid response data from '+self.server[0])
		if addr[0] != self.server[1]: raise Exception('PingServer::setup_block: invalid response server from '+self.server[0])
		self.block_size = len(data)
		self.empty_block = self.null_block()
		
	def setup(self):
		print "testing server:",self.server[0]
		ID = random.getrandbits(32)
		self.setup_timeout(ID)
		self.setup_block(ID)

	def stop(self):
		print "stopping ping server"
		self.running = 0

	def run(self):
		print "starting ping server"
		while self.running:
			start_blocks = self.blocks # updated asynchronously
			ready = select.select([self.socket], [], [], self.timeout)
			if ready[0] == []: # timeout
				if start_blocks != 0 and self.blocks != 0:
					print self.server[0],"timed out"
				continue

			addr,block_id,data = ping.recv_ping(self.socket,self.timeout)
			self.process_block(addr[0],block_id,data)

	def process_block(self, addr, ID, data):
		while len(self.queued_events[ID]):
			handler,timer,args = self.queued_events[ID].popleft()
			if not timer.is_alive(): continue
			timer.cancel()
			
			if handler == self.write_block_timeout:
				print self.server[0],'(block %d)'%ID,': updated'
				data = args[1]
			if handler == self.read_block_timeout:
				print self.server[0],'(block %d)'%ID,': read'
				callback,cb_args = args[1],args[2]
				if len(data) > 0: callback(ID,data,*cb_args)
				else:             callback(ID,self.null_block(),*cb_args)
			if handler == self.delete_block_timeout:
				print self.server[0],'(block %d)'%ID,': deleted'
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
		if ID == 0: raise Exception('write_block: invalid block ID (0)')
		if data == '%c'%0 * len(data): return self.delete_block(ID,blocking)
		t = self.event_insert(ID,self.write_block_timeout,[ID,data[:self.block_size]])
		if blocking: t.join()
		return t

	def delete_block(self, ID, blocking = False):
		if ID == 0: raise Exception('delete_block: invalid block ID (0)')
		t = self.event_insert(ID,self.delete_block_timeout,[ID])
		if blocking: t.join()
		return t

	def read_block(self, ID, callback, cb_args = [], blocking = False):
		if ID == 0: raise Exception('read_block: invalid block ID (0)')
		t = self.event_insert(ID,self.read_block_timeout,[ID,callback,cb_args])
		if blocking: t.join()
		return t

	def read_block_timeout(self, ID, callback, cb_args):
		#print "read block executing"
		callback(ID,self.null_block(),*cb_args)

	def delete_block_timeout(self, ID):
		# do nothing; we're marked invalid anyhow
		#print "rem block executing"
		pass

	def write_block_timeout(self, ID, data):
		#print "write block executing"
		self.blocks = self.blocks + 1
		# force update queue (as if packet arrived)
		self.process_block(self.server[1], ID, data)

	def sync_read(self, ID, blocks=1):
		data = {}
		timers = []
		for x in range(ID,ID+blocks,1):
			timers.append(self.read_block(x, self.sync_read_callback, [data], False))
		for x in timers: x.join()

		block = ''
		for x in range(ID,ID+blocks,1):
			if len(data[x]) < self.block_size:
				data[x] = data[x] + self.null_block()[len(data[x]):]
			block = block + data[x]
		return ID,block

	def sync_read_callback(self, ID, data, data_store):
		#print 'Callback:',len(data),data
		data_store[ID] = data

	def index_to_block(self, index): # byte 0 stored in block 1
		return int(math.floor(float(index) / self.block_size)) + 1

	def read(self, index, length):
		init_block = int(math.floor(float(index)	/ self.block_size)) + 1 # byte 0 is in block 1
		fini_block = int(math.ceil (float(index+length) / self.block_size)) + 1

		ID,data = self.sync_read(init_block, fini_block)
		data = data[index % self.block_size:][:length]
		return data

	def block_merge(self, old_data, new_data, index = 0):
		if index >= self.block_size: raise Exception('block_merge: invalid index ('+str(index)+')')
		old_data = old_data[:self.block_size]
		new_data = new_data[:self.block_size-index]
		data = old_data[:index] + new_data + old_data[index+len(new_data):]
		return data

	def write_blocks(self, index, data):
		endex = index + len(data)
		init_index = (index % self.block_size)
		fini_index = (endex % self.block_size)
		init_block = (index / self.block_size) + 1 # byte 0 is in block 1
		fini_block = (endex / self.block_size) + 1

		timers = []
		if init_index == 0:
			start_block = data[:self.block_size]
		else:
			ID,start_block = self.sync_read(init_block)
			start_block = self.block_merge(start_block,data,init_index)
		timers.append(self.write_block(init_block,start_block,False))
		if init_block == fini_block: return timers

		data = data[self.block_size - init_index:]
		for x in range(init_block+1,fini_block-1,1):
			timers.append(self.write_block(x,data,False))
			data = data[self.block_size:]
		
		if fini_index == 0:
			end_block = data[:self.block_size]
		else:
			ID,end_block = self.sync_read(fini_block)
			end_block = self.block_merge(end_block,data,0)
		timers.append(self.write_block(fini_block,end_block,False))
		return timers

	def sync_write(self, index, data):
		for x in write_blocks(index,data): x.join()

def print_block(ID, data):
	print '----- print block -----'
	print 'block',ID,'bytes',len(data)
	print data
	print '----- print block -----'

def humanize_bytes(bytes, precision=1):
	# by Doug Latornell
	# http://code.activestate.com/recipes/577081-humanized-representation-of-a-number-of-bytes/
	abbrevs = (
		(1<<50L, 'PB'),
		(1<<40L, 'TB'),
		(1<<30L, 'GB'),
		(1<<20L, 'MB'),
		(1<<10L, 'kB'),
		(1, 'bytes')
	)
	if bytes == 1:
		return '1 byte'
	for factor, suffix in abbrevs:
		if bytes >= factor:
			break
	return '%.*f %s' % (precision, bytes / factor, suffix)

if __name__ == "__main__":
	try:
		PS = PingServer("google.com")
		#PS = PingServer("172.16.1.1")
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
		print 'traffic:',ping.ping_count,'pings ('+humanize_bytes(ping.ping_bandwidth)+')'
		sys.exit(1)
		
