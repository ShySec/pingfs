import ping_server,time,struct,sys

"""
TFS_Block:
[00:04-byte] block id
[04:_______] block data

TFS_Block1:	
[00:04-byte] block id = 1
[04:01-byte] version
[05:03-byte] identifier

TFS_Block1_v1:
[00:04-byte] block id = 1
[04:01-byte] version = 1
[05:03-byte] reserved = 0
[08:04-byte] identifier
[0c:04-byte] block size ( >8 )
[10:04-byte] block count
[14:04-byte] root block

TFSv1_Directory:
[00:04-byte] entry count
[04:_______] directory entries

TFSv1_Entry:
[00:02-byte] entry size
[02:02-byte] reserved
[04:04-byte] entry type
[08:04-byte] attribute
[0c:04-byte] block id
[10:04-byte] block length
[14:_______] name
"""

class TFSv1_Block:
	def __init__(self,block_id,data):
		self.ID		= block_id
		self.data	= data

class TFSv1_Root(TFSv1_Block):
	def __init__(self,block_id,data):
		TFSv1_Block.__init__(self,block_id,data)
		self.parse()

	def parse(self):
		if len(self.data) < struct.calcsize('4B4L'):
			raise Exception('TFS::Block::Root: invalid root block')

		self.version	= struct.unpack('B',self.data[0:1])[0]
		self.reserved	= 0 #struct.unpack('3B',self.data[1:4])
		self.ident,self.size,self.count,self.root = struct.unpack('4L',self.data[4:20])
		return self

class TFSv1_Directory:
	def __init__(self,data):
		if len(data) < struct.calcsize('L'):
			print 'invalid directory'
		self.entry_count = struct.unpack('L',data[0:4])
		self.entry_data = data[4:]
		self.entries = []
		print 'TFSv1_Directory:',self.entry_count,'entries'

	def process_entry(self):
		print 'processing entry'
		entry = TFSv1_Entry(self.entry_data)
		self.entries.append(entry)
		self.entry_data = self.entry_data[entry.size:]
		print 'processed entry'
		entry.display()

class TFSv1_Entry:
	def pad_entry(self,data):
		if len(data) < struct.calcsize('5LH'):
			data = data + (struct.calcsize('5LH')-len(data))*struct.pack('B',0)
		return data
	
	def __init__(self,data):
		data = self.pad_entry(data)

		self.size		= struct.unpack('H',data[ 0:2])[0]
		self.type		= struct.unpack('L',data[ 4:8])[0]
		self.attr		= struct.unpack('L',data[ 8:12])[0]
		self.entry_id	= struct.unpack('L',data[12:16])[0]
		self.entry_size	= struct.unpack('L',data[16:20])[0]
		self.name		= data[20:self.size] # [20:20+self.size-20]
		print 'TFSv1_Entry:',self.name

	def display(self):
		print '----- Entry Data -----'
		print 'Type:',self.type
		print 'Attr:',self.attr
		print 'Size:',self.entry_size
		print 'Name:',self.name
		print '----------------------'

def RootBlock_v1(identifier, block_size, block_count, root_block):
	block_version	= struct.pack('B', 1)
	block_reserved	= struct.pack('3B',0,0,0)
	block_identifier= struct.pack('L', identifier)
	block_size		= struct.pack('L', block_size)
	block_count		= struct.pack('L', block_count)
	block_root		= struct.pack('L', root_block)

	block_data = block_version + block_reserved + block_identifier + block_size + block_count + block_root
	return block_data

class PingFS:
	def __init__(self,server):
		try:
			self.server = ping_server.PingServer(server)
			self.server.setup()
			self.server.start()
		
			self.block_count = 0x1000000
			self.block_size = self.server.block_size
			self.server.write_block(1,RootBlock_v1(0,self.block_size,self.block_count,2),True)
		except:
			print 'General Exception'
			from traceback import print_exc
			print_exc()

	def stop(self):
		self.server.stop()

	def info(self):
		ID,data = self.server.sync_read(1)
		root = TFSv1_Root(ID,data)
		print '----- Root Block -----'
		print 'Version:',		root.version
		print 'Identifier:',	root.ident
		print 'Block Size:',	root.size
		print 'Block Count:',	root.count
		print 'Filesystem Max:',root.size*root.count/(1024*1024*8),'MB'
		print 'Root Entry:',	root.root
		print '----------------------'

	def root_block(self):
		data = self.server.read(0,struct.calcsize('4B4L'))
		if len(data) < struct.calcsize('4B4L'):
			print 'invalid root block'
			return
		version = struct.unpack('B',data[0:1])
		reserved = 0
		ident = struct.unpack('L',data[4:8])
		size,count,root = struct.unpack('3L',data[8:20])
#		version,reserved,ident,size,count,root = struct.unpack('B3cLLLL',data[0:struct.calcsize('4B4L')])
		return (1,version,reserved,ident,size,count,root)

	def root_node(self):
		root_block_tuple = self.root_block()
		ID,root_node_data = self.server.sync_read(root_block_tuple[6])
		root_node = TFSv1_Directory(root_node_data)

		entry_count = struct.unpack('L',root_node_data[0:4])[0]
		for i in range(0,entry_count): root_node.process_entry()
		return (ID,entry_count,root_node_data[4:])

	def read_directory(self, pathname):
		ID,count,data = self.root_node()





#def ls(FS, pathname):
	

if __name__ == '__main__':
	try:
		FS = PingFS("www.google.com")
		time.sleep(2)
		FS.info()
		time.sleep(1)

		print 'faking directories'
		directory = struct.pack('L',2)
		e1_name = 'apples'
		e2_name = 'bananas'
		entry_1 = struct.pack('HHLLLL%ds'%len(e1_name),20+len(e1_name),0,1,0,5,1,e1_name)
		entry_2 = struct.pack('HHLLLL%ds'%len(e2_name),20+len(e2_name),0,2,1,10,2,e2_name)

		print 'adding directories'
		FS.server.write_block(2,directory+entry_1+entry_2)

		time.sleep(2)
		FS.root_node()

		time.sleep(500)
	except KeyboardInterrupt:
		print "Keyboard Interrupt"
	except Exception:
		print 'General Exception'
		from traceback import print_exc
		print_exc()
	finally:
		FS.stop()
		sys.exit(1)
