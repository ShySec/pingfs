import ping, threading, time, socket, select, sys, struct
import binascii, threading, collections, math, random
import ping_server


class PingDisk():
	def __init__(self, d_addr, block_size=1024, timeout=2):
		self.server = ping_server.PingServer(d_addr,block_size,timeout)
		self.server.setup()
		self.server.start()

	def stop(self):
		self.server.stop()

	def size(self):
		return self.server.block_size * (1<<31)

	def block_size(self):
		return self.server.block_size

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
		blocks = range(init_block,fini_block+1)
		for x in blocks: timers.append(self.read_block(x,[data]))
		for x in timers: x.join()

		result = ''
		for x in blocks: result = result + data[x]
		return result

	def __read_callback(self, ID, data, data_store):
		#print 'Callback:',len(data),data
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
		#print 'writing block',ID,': ',data
		return self.server.write_block(ID,data,blocking)

	def write_blocks(self, index, data):
		endex = index + len(data)
		block_size = self.server.block_size
		init_index = (index % self.server.block_size)
		fini_index = (endex % self.server.block_size)
		init_block = (index / self.server.block_size) + 1 # byte 0 is in block 1
		fini_block = (endex / self.server.block_size) + 1

		#print 'write_blocks',init_block,',',fini_block

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

if __name__ == "__main__":
        Disk = None
	try:
		#Disk = PingDisk("10.44.0.1",1024)
		Disk = PingDisk("google.com",1024)
		data = "1234567890123456789_123456789012345"
		Disk.write(0,data)
		time.sleep(1)
		rData = Disk.read(0,len(data))
		print 'length:',len(data),'vs',len(rData)
		print 'data = ',rData
		Disk.write(10,'abcdefghijk')
		time.sleep(2)
		rData = Disk.read(0,len(data))
		time.sleep(2)
		rData = Disk.read(2,len(data))
		print 'length:',len(rData)
		print 'data = ',rData
		time.sleep(2)
		rData = Disk.read(2,10)
		print 'length:',len(rData)
		print 'data = ',rData
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
		print 'traffic:',ping.ping_count,'pings ('+ping.humanize_bytes(ping.ping_bandwidth)+')'
		sys.exit(1)
		
