import time, struct, sys, stat, logging
import ping, ping_disk, ping_reporter

log = ping_reporter.setup_log('PingFileSystem')

"""
PingFS_File
[00: 4] size
[04: 4] type
[08: 4] attributes
[0c: 4] reserved
[10:__] data

PingFS_Directory(PingFS_File)
[10: 4] entry count
[14:__] entries

PingFS_DirEntry
[00: 2] name length
[02:__] name
"""

def makePingNode(data):      
	pnode = PingNode()
	pnode.deserialize(data)
	return pnode

def makePingFile(data):
	pfile = PingFile()
	pfile.deserialize(data)
	return pfile

def makePingDirent(data):
	pdir = PingDirent()
	pdir.deserialize(data)
	return pdir

def makePingDirectory(data):
	pdir = PingDirectory()
	pdir.deserialize(data)
	return pdir

def interpretFile(data):
	pf = makePingFile(data)
	if pf.type == stat.S_IFDIR: return makePingDirectory(data)
	return pf

def interpretSize(data):
	inode,size = struct.unpack('2L',data[:struct.calcsize('2L')])
	return size

class PingNode():
	layout = 'L'
	overhead = struct.calcsize(layout)

	def __init__(self,inode=0):
		self.inode = inode

	def get_parts(self,data,size):
		if len(data) < size: return ''
		return data[:size],data[size:]

	def serialize(self):
		log.trace('%s::serialize'%self.__class__.__name__)
		return struct.pack(PingNode.layout,self.inode)

	def deserialize(self,data):
		log.trace('%s::deserialize'%self.__class__.__name__)
		layout,overhead = PingNode.layout,PingNode.overhead
		if len(data) < overhead: raise Exception('PingFS::node: invalid deserialize data')
		self.inode = struct.unpack(layout,data[:overhead])[0]
		return data[overhead:]

class PingFile(PingNode):
	layout = '4L'
	overhead = struct.calcsize(layout)
	file_header = overhead + PingNode.overhead

	def __init__(self,inode=0,name=''):
		PingNode.__init__(self,inode)
		self.type = stat.S_IFREG
		self.mode = 0733
		self.name = name
		self.attrs = {}
		self.data = ''
		self.attr = 0
	
	def get_attr(self):
		return self.attrs

	def size(self):
		return PingFile.file_header + len(self.data)

	def links(self):
		return 1

	def serialize(self):
		node_hdr = PingNode.serialize(self)
		layout,overhead = PingFile.layout,PingFile.overhead
		file_hdr = struct.pack(layout,len(self.data),self.type,self.attr,0)
		return node_hdr + file_hdr + self.data

	def deserialize(self,data):
		data = PingNode.deserialize(self,data)
		layout,overhead = PingFile.layout,PingFile.overhead
		if len(data) < overhead: raise Exception('PingFS::file: invalid deserialize data')
		size,self.type,self.attr,res = struct.unpack(layout,data[:overhead])
		self.data = data[overhead:overhead+size]
		#print 'PingFile::name(',self.name,'),size,type,attr:',size,self.type,self.attr
		return data[overhead+size:]

class PingDirent(PingNode):
	layout = 'H'
	overhead = struct.calcsize(layout)

	def __init__(self):
		pass

	def size(self):
		return PingNode.overhead + PingDirent.overhead + len(self.name)

	def serialize(self):
		node_hdr = PingNode.serialize(self)
		layout,overhead = PingDirent.layout,PingDirent.overhead
		header = struct.pack(layout,len(self.name))
		return node_hdr + header + self.name

	def deserialize(self,data):
		data = PingNode.deserialize(self,data)
		layout,overhead = PingDirent.layout,PingDirent.overhead
		if len(data) < overhead: raise Exception('PingFS::dirent: invalid deserialize')
		size = struct.unpack(layout,data[:overhead])[0]
		data = data[overhead:]
		if len(data) < size: raise Exception('PingFS::dirent: invalid directory object (%d,%d)'
											 %(len(data),size))
		self.name = data[:size]
		#print 'PingDirent::inode,len,name',self.inode,len(self.name),self.name
		return data[size:]

class PingDirectory(PingFile):
	layout = 'L'
	overhead = struct.calcsize(layout)
	
	def __init__(self,inode=0,name=''):
		PingFile.__init__(self,inode,name)
		self.type = stat.S_IFDIR
		self.entries = []
		self.mode = 0766

	def size(self):
		size = PingFile.overhead + PingDirectory.overhead
		for x in self.entries:
			size = size + x.size()
		return size

	def links(self):
		return len(self.entries) + 1
		
	def add_node(self,node):
		self.del_node(node.name)
		dirent = PingDirent()
		dirent.inode = node.inode
		dirent.name = node.name
		self.entries.append(dirent)

	def del_node(self,name):
		for x in self.entries:
			if x.name == name:
				del x

	def get_dirent(self,name):
		for x in self.entries:
			if x.name == name:
				return x
		return None

	def serialize(self):
		file_hdr = PingFile.serialize(self)
		layout,overhead = PingDirectory.layout,PingDirectory.overhead
		header = struct.pack(layout, len(self.entries))

		data = ''
		for x in self.entries: data = data + x.serialize()
		return file_hdr + header + data

	def deserialize(self,data):
		self.entries = []
		data = PingFile.deserialize(self,data)
		layout,overhead = PingDirectory.layout,PingDirectory.overhead
		if len(data) < overhead: raise Exception('PingFS::dir: invalid deserialize')
		count = struct.unpack(layout,data[:overhead])[0]
		data = data[overhead:]
		for x in range(0,count):
			dirent = PingDirent()
			data = dirent.deserialize(data)
			self.add_node(dirent)
		return data

class PingFS:
	def __init__(self,server):
		try:
			self.disk = ping_disk.PingDisk(server)
			root = PingDirectory(0,'/')
			self.disk.write(0,root.serialize())

		except:
			print 'General Exception'
			from traceback import print_exc
			print_exc()

	def read_inode(self,inode,length=0):
		log.debug('PingFS::read_inode: inode=%d length=%d'%(inode,length))
		if length == 0:
			block_size = max(self.disk.block_size(),PingFile.file_header)
		data = self.disk.read(inode,block_size)
		size = interpretSize(data)

		if size > len(data):
			data = self.disk.read(inode,size)
		return data

	def read_as_file(self,inode):
		log.debug('PingFS::read_as_file: inode=%d'%inode)
		data = self.read_inode(inode)
		pfile = makePingFile(data)
		return pfile

	def read_as_dir(self,inode):
		log.debug('PingFS::read_as_dir: inode=%d'%inode)
		data = self.read_inode(inode)
		pdir = makePingDirectory(data)
		if not (pdir.type & stat.S_IFDIR):
			raise Exception('read_as_dir: %s (%d,%d) -> %x %d'%(pdir.name,inode,len(data),pdir.type,len(pdir.entries)))
		return pdir

	def get(self, path):
		log.notice('PingFS::get %s'%path)
		if path == '/' or path == '': return self.read_as_dir(0)
		parts = path.rsplit('/',1)
		if len(parts) != 2: raise Exception('PingFS::get_file: invalid path: %s'%path)
		rPath,fName = parts[0],parts[1]
		pDir = self.get(rPath)
		if pDir and pDir.type == stat.S_IFDIR:
			pEntry = pDir.get_dirent(fName)
			if pEntry:
				data = self.read_inode(pEntry.inode)
				pFile = interpretFile(data)
				return pFile
		return None

	def add(self,node):
		log.notice('PingFS::add %s at %d'%(node.name,node.inode))
		self.disk.write(node.inode,node.serialize())
		
	def stop(self):
		log.info('PingFS: stopping')
		self.disk.stop()

def init_fs(FS):
	log.notice('building root directory')
	d1 = PingDirectory(0,'/')
	d1.deserialize(d1.serialize())

	log.notice('building file (apples)')
	f1 = PingFile(1*1024,'apples')
	f1.data = 'delicious apples'
	f1.deserialize(f1.serialize())

	log.notice('adding node (/apples)')
	d1.add_node(f1)
	d1.deserialize(d1.serialize())

	log.notice('building sub-directory (l1)')
	d2 = PingDirectory(2*1024,'l1')

	log.notice('adding node (/l1)')
	d1.add_node(d2)
	d1.deserialize(d1.serialize())

	log.notice('building file (banana)')
	f2 = PingFile(3*1024,'banana')
	f2.data = 'ripe yellow bananas'
	f2.deserialize(f2.serialize())

	log.notice('adding node (/l1/banana)')
	d2.add_node(f2)
	d2.deserialize(d2.serialize())

	FS.add(d1)
	FS.add(d2)
	FS.add(f1)
	FS.add(f2)

	log.notice('test filesystem initialized')

def test_fs(FS):
	root = FS.read_as_dir(0)
	log.info('- read as dir / -----------------------------')
	log.info('%d' % root.type)
	log.info('---------------------------------------------')
	
	root = FS.get('')
	log.info('- get "" ------------------------------------')
	log.info('%d' % root.type)
	log.info('---------------------------------------------')

	root = FS.get('/')
	log.info('- get / -------------------------------------')
	if not root: log.info('missed /')
	else:		log.info('%s %d'%(root.name,root.inode))
	log.info('---------------------------------------------')

	sfile = FS.get('/apples')
	log.info('- get /apples -------------------------------')
	if not sfile: log.info('missed /apples')
	else:		log.info('%s %d'%(sfile.name,sfile.inode))
	log.info('---------------------------------------------')

	sub = FS.get('/l1')
	log.info('- get /l1 -----------------------------------')
	if not sub: log.info('missed /l1')
	else: 		log.info('%s %d'%(sub.name,sub.inode))
	log.info('---------------------------------------------')

	sfile = FS.get('/l1/banana')
	log.info('- get /l1/banana ----------------------------')
	if not sfile: log.info('missed /l1/banana')
	else:		log.info('%s %d'%(sfile.name,sfile.inode))
	log.info('---------------------------------------------')

if __name__ == '__main__':
	FS = None
	try:
		ping_reporter.start_log(log,logging.DEBUG)
		#ping_reporter.start_log(log,logging.TRACE)
		server = ping.select_server()
		FS = PingFS(server)
		init_fs(FS)
		test_fs(FS)

	except KeyboardInterrupt:
		print "Keyboard Interrupt"
	except Exception:
		print 'General Exception'
		from traceback import print_exc
		print_exc()
	finally:
		if FS: FS.stop()
		sys.exit(1)
