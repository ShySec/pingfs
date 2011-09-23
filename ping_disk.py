import ping, threading, time, socket, select, sys, struct, logging
import binascii, threading, collections, math, random
import ping, ping_server, ping_reporter

log = ping_reporter.setup_log('PingDisk')

class PingDisk():
	def __init__(self, d_addr, block_size=1024, timeout=2):
		self.server = ping_server.PingServer(d_addr,block_size,timeout)
		self.server.setup()
		self.server.start()

	def stop(self):
		self.server.stop()

	def size(self):
		return self.server.block_size * (1<<28)

	def block_size(self):
		return self.server.block_size

	def region_size(self):
		return max(2,4096/self.block_size())

	def read_block(self, ID, datastore, blocking=False):
		timer = self.server.read_block(ID, self.__read_callback, datastore, False)
		if not blocking: return timer
		timer.join()

	def read_block_sync(self, ID):
		data = {}
		self.read_block(ID,[data],True)
		return data[ID]

	def read_blocks(self, init_block, fini_block):
		data = {}
		timers = []
		result = ''
		blocks = range(init_block,fini_block+1)
		for x in blocks: timers.append(self.read_block(x,[data]))
		for x in timers: x.join()
		for x in blocks: result = result + data[x]
		return result

	def __read_callback(self, ID, data, data_store):
		log.trace('PingDisk::read::callback: ID=%d bytes=%d'%(ID,len(data)))
		data_store[ID] = data

	def read(self, index, length):
		endex = index + length
		init_index = (index % self.server.block_size)
		fini_index = init_index + length
		init_block = (index / self.server.block_size) + 1
		fini_block = (endex / self.server.block_size) + 1

		data = self.read_blocks(init_block,fini_block)
		return data[init_index:fini_index]

	def __block_merge(self, old_data, new_data, index = 0):
		if index >= self.server.block_size: raise Exception('block_merge: invalid index ('+str(index)+')')
		old_data = old_data[:self.server.block_size]
		new_data = new_data[:self.server.block_size-index]
		data = old_data[:index] + new_data + old_data[index+len(new_data):]
		return data

	def write_block(self, ID, data, blocking=False):
		log.trace('PingDisk::write_block: ID=%d bytes=%d'%(ID,len(data)))
		return self.server.write_block(ID,data,blocking)

	def write_blocks(self, index, data):
		endex = index + len(data)
		block_size = self.server.block_size
		init_index = (index % self.server.block_size)
		fini_index = (endex % self.server.block_size)
		init_block = (index / self.server.block_size) + 1 # byte 0 is in block 1
		fini_block = (endex / self.server.block_size) + 1

		log.trace('PingDisk::write_blocks: blocks %d-%d'%(init_block,fini_block))

		timers = []
		if init_index == 0:
			start_block = data[:block_size]
		else:
			start_block = self.read_block_sync(init_block)
			start_block = self.__block_merge(start_block,data,init_index)
		timers.append(self.write_block(init_block,start_block))
		if init_block == fini_block: return timers

		data = data[self.server.block_size - init_index:]
		for x in range(init_block+1,fini_block):
			timers.append(self.write_block(x,data[:block_size]))
			data = data[block_size:]
		
		if fini_index != 0:
			end_block = self.read_block_sync(fini_block)
			end_block = self.__block_merge(end_block,data,0)
			timers.append(self.write_block(fini_block,end_block))
		return timers

	def write(self, index, data, blocking=True):
		timers = self.write_blocks(index,data)
		if not blocking: return timers
		for x in timers: x.join()

	def free_blocks(self, timeout=1):
		blocks = ping_server.live_blocks(self.server,timeout)
		log.debug('live_blocks: %s'%blocks)
		if blocks: return ping_server.free_blocks(blocks)
		return None

	def used_blocks(self, timeout=1):
		blocks = ping_server.live_blocks(self.server,timeout)
		if blocks: return ping_server.used_blocks(blocks)
		return None

	def get_block_region(self, blocks=1, timeout=1):
		free = self.free_blocks(timeout)
		log.debug('get_block_region: %d blocks  %d timeout'%(blocks,timeout))
		if not free: return None
		max_blockid = (1<<28)

		# 1) try allocating a new prime region
		top_node = max(free.keys())
		reg_size = self.region_size()
		if max_blockid - top_node > reg_size:
			return reg_size * int(math.ceil(1.0*top_node/reg_size))

		# 2)try minimal sufficiently large region
		region = [(v,k) for (k,v) in free.iteritems() if v >= blocks]
		if region: return min(region)[1]
		return None

	def get_region(self, bytes, timeout=1):
		log.debug('get_region: %d bytes  %d timeout'%(bytes,timeout))
		blocks = int(math.ceil(1.0*bytes / self.block_size())) # to blocks
		region = self.get_block_region(blocks,timeout)         # <------>
		if region: region *= self.block_size()                 # to bytes
		log.debug('get_region: offset %d'%region)
		return region


if __name__ == "__main__":
	Disk = None
	try:
		ping_reporter.start_log(log,logging.DEBUG)
		server = ping.select_server(log)
		Disk = PingDisk(server,4)
		#ping.drop_privileges()
		data = "1234567890123456789_123456789012345"
		Disk.write(0,data)
		time.sleep(1)
		rData = Disk.read(0,len(data))
		log.info('length: %d vs %d'%(len(data),len(rData)))
		log.info('data = %s',rData)
		Disk.write(10,'abcdefghijk')
		time.sleep(2)
		rData = Disk.read(0,len(data))
		time.sleep(2)
		rData = Disk.read(2,len(data))
		log.info('length: %d vs %d'%(len(data),len(rData)))
		log.info('data = %s',rData)
		time.sleep(2)
		rData = Disk.read(2,10)
		log.info('length: %d vs %d'%(len(data),len(rData)))
		log.info('data = %s',rData)

		free_node = Disk.get_region(20)
		log.info('get_region = %s'%free_node)

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
		if Disk: Disk.stop()
		log.info('traffic: %d pings (%s)'
				%(ping.ping_count,ping_reporter.humanize_bytes(ping.ping_bandwidth)))
		sys.exit(1)
		
