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
		event = self.server.read_block(ID, self.__read_callback, datastore, False)
		if not blocking: return event
		event.wait()

	def read_block_sync(self, ID):
		data = {}
		self.read_block(ID,[data],True)
		return data[ID]

	def read_blocks(self, init_block, fini_block):
		data = {}
		events = []
		result = ''
		blocks = range(init_block,fini_block+1)
		log.debug('PingDisk::read_blocks: blocks %d-%d'%(init_block,fini_block))
		for x in blocks: events.append(self.read_block(x,[data]))
		for x in events: x.wait()
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

		if 0 == endex % self.server.block_size:
			fini_block = max(init_block,fini_block-1)
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
		log.debug('PingDisk::write_blocks: blocks %d-%d'%(init_block,fini_block))

		events = []
		if init_index == 0:
			start_block = data[:block_size]
		else:
			start_block = self.read_block_sync(init_block)
			start_block = self.__block_merge(start_block,data,init_index)
		events.append(self.write_block(init_block,start_block))
		if init_block == fini_block: return events

		data = data[self.server.block_size - init_index:]
		for x in range(init_block+1,fini_block):
			events.append(self.write_block(x,data[:block_size]))
			data = data[block_size:]
		
		if fini_index != 0:
			end_block = self.read_block_sync(fini_block)
			end_block = self.__block_merge(end_block,data,0)
			events.append(self.write_block(fini_block,end_block))
		return events

	def write(self, index, data, blocking=True):
		events = self.write_blocks(index,data)
		if not blocking: return events
		for x in events: x.wait()

	def delete_blocks(self, index, length):
		endex = index + length
		init_block = (index / self.server.block_size) + 1 # byte 0 is in block 1
		fini_block = (endex / self.server.block_size) + 1
		log.debug('PingDisk::delete_blocks: blocks %d-%d'%(init_block,fini_block))

		events = []
		for x in range(init_block,fini_block+1):
			events.append(self.server.delete_block(x))
		return events

	# delete operates at block-level boundaries
	def delete(self, index, length, blocking=False):
		log.debug('PingDisk::delete: index=%d for %d bytes'%(index,length))
		events = self.delete_blocks(index,length)
		if not blocking: return events
		for x in events: x.wait()

	def free_blocks(self, timeout=None):
		blocks = ping_server.live_blocks(self.server,timeout)
		log.debug('live_blocks: %s'%blocks)
		return ping_server.free_blocks(blocks)

	def used_blocks(self, timeout=None):
		blocks = ping_server.live_blocks(self.server,timeout)
		log.debug('used_blocks: %s'%blocks)
		if blocks: return ping_server.used_blocks(blocks)
		return {}

	def get_block_region(self, blocks=1, timeout=None):
		free = self.free_blocks(timeout)
		log.debug('get_block_region: %d blocks'%(blocks))
		log.debug('frees -> %s'%free)
		if not free: return None
		max_blockid = (1<<28)

		# 1) allocate an encompassing span of regions
		top_node = max(free.keys())
		reg_size = self.region_size() * int(1 + blocks/self.region_size())
		top_node = reg_size * int(1 + top_node/reg_size)
		if max_blockid - top_node > reg_size: return top_node

		# 2)try minimal sufficiently large region
		region = [(v,k) for (k,v) in free.iteritems() if v >= blocks]
		if region: return min(region)[1]
		return None

	def timeout(self):				return self.server.timeout()
	def safe_timeout(self):			return self.server.safe_timeout()
	def byte_to_block(self, byte):	return int(math.ceil(1.0*byte/self.block_size()))
	def block_to_byte(self, block):	return block * self.block_size()

	def get_region(self, bytes, timeout=None, target=None):
		log.debug('get_region: %d bytes'%(bytes))
		if not timeout: timeout = self.safe_timeout()
		if target: return self.test_region(target,bytes,timeout)
		block = self.byte_to_block(bytes)              # to blocks
		region = self.get_block_region(block,timeout)  # <------>
		if region: region = self.block_to_byte(region) # to bytes
		log.debug('get_region: allocated region %d (%d bytes)'%(region,bytes))
		return region

	def test_region(self, start, end, length, timeout=None):
		if not timeout: timeout = self.safe_timeout()
		log.debug('test_region: region=%d-%d length=%d'%(start,end,length))
		if length < end: return start # smaller block
		collision = [start+end-1,start+length-1]
		collision2 = [collision[0],collision[1]]
		collision[0] = self.byte_to_block(collision[0])
		collision[1] = self.byte_to_block(collision[1])
		if collision[0] == collision[1]: return start # same block
		used = self.used_blocks(timeout)
		if not used: # 0 used blocks implies no root directory...
			log.exception('test_region: used blocks returned nil')
			raise Exception('test_region: used blocks returned nil')

		for x in used:
			if x <= collision[0]: continue # used block before test region (no collision)
			if x  > collision[1]: continue # used block begins after collision space
			log.debug('test_region: collision at node %d'%x)
			return False
		return start
		

if __name__ == "__main__":
	Disk = None
	try:
		#ping_reporter.start_log(log,logging.DEBUG)
		ping_reporter.enableAllLogs(logging.DEBUG)
		server = ping.select_server(log)
		Disk = PingDisk(server,4)
		#ping.drop_privileges()

		if 1:
			data = "1234567890123456789_123456789012345"
			log.info('blind disk read')
			rData = Disk.read(0,50)
			log.info('1-length: 50 requested -> %d'%(len(rData)))
			if rData != '\0'*50: log.exception('invalid rData: %s'%rData)
			else: log.info('success')
			time.sleep(5)

			log.info('writing %d bytes'%len(data))
			Disk.write(0,data)
			time.sleep(5)
			rData = Disk.read(0,len(data))
			log.info('2-length: %d vs %d'%(len(data),len(rData)))
			if rData != data: log.exception('invalid rData: %s'%rData)
			else: log.info('success')

			wData = 'abcdefghijk'
			Disk.write(10,wData)
			time.sleep(2)
			rData = Disk.read(0,len(data))
			data = data[0:10] + wData + data[10+len(wData):]
			log.info('3-length: %d vs %d'%(len(data),len(rData)))
			if rData != data: log.exception('invalid rData: %s'%rData)
			else: log.info('success')

			time.sleep(2)
			data = data[2:] + '\0\0'
			rData = Disk.read(2,len(data))
			log.info('4-length: %d vs %d'%(len(data),len(rData)))
			if rData != data: log.exception('invalid rData: %s'%rData)
			else: log.info('success')

			time.sleep(2)
			data = data[0:10]
			rData = Disk.read(2,10)
			log.info('5-length: %d vs %d'%(len(data),len(rData)))
			if rData != data: log.exception('invalid rData: %s'%rData)
			else: log.info('success')

		if 1:
			#free_node = Disk.get_region(1500)
			#log.info('get_region = %s'%free_node)

			strA = 'A'
			strB = 'B'*4096*4
			Disk.write(0,strA)
			Disk.write(5000,strB)

			time.sleep(3)
			#log.info('region A [%d] and B [%d] written'%(len(strA),len(strB)))
			readA = Disk.read(0,len(strA))
			readB = Disk.read(5000,len(strB))
			log.info('region A and B read')
			if readA != strA: log.error('corruption in region A (%d bytes)'%len(readA))
			else:             log.debug('region A read successfully')
			if readB != strB: log.error('corruption in region B (%d bytes)'%len(readB))
			else:             log.debug('region B read successfully')

		time.sleep(10)
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
		
