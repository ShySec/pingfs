import time, struct, sys, stat, logging
import ping, ping_disk, ping_reporter

log = ping_reporter.setup_log('PingFileSystem')

"""
PingFS_File
[00: 4] size
[04: 4] type
[08: 2] uid
[0a: 2] gid
[0c: 2] mode
[0e: 2] reserved
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
		self.parent = None
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
	layout = '2L3H2x'
	overhead = struct.calcsize(layout)
	file_header = overhead + PingNode.overhead

	def __init__(self,name='',inode=0):
		PingNode.__init__(self,inode)
		self.type = stat.S_IFREG
		self.mode = 0666
		self.name = name
		self.data = ''
		self.uid = 0
		self.gid = 0
	
	def get_attr(self):
		return self.attrs

	def size(self):
		return PingFile.file_header + len(self.data)

	def links(self):
		return 1

	def serialize(self):
		self.disk_size = self.size()
		node_hdr = PingNode.serialize(self)
		layout,overhead = PingFile.layout,PingFile.overhead
		file_hdr = struct.pack(layout,len(self.data),self.type,self.uid,self.gid,self.mode)
		return node_hdr + file_hdr + self.data

	def deserialize(self,data):
		data = PingNode.deserialize(self,data)
		layout,overhead = PingFile.layout,PingFile.overhead
		if len(data) < overhead: raise Exception('PingFS::file: invalid deserialize data')
		size,self.type,self.uid,self.gid,self.mode = struct.unpack(layout,data[:overhead])
		self.data = data[overhead:overhead+size]
		self.disk_size = self.size()
		#print 'PingFile::name(',self.name,'),size,type,attr:',size,self.type,self.attr
		return data[overhead+size:]

class PingDirent(PingNode):
	layout = 'H'
	overhead = struct.calcsize(layout)

	def __init__(self):
		PingNode.__init__(self,None)

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
	
	def __init__(self,name='',inode=0):
		PingFile.__init__(self,name,inode)
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
		if node.parent: node.parent.del_node(node.name,node)
		self.del_node(node.name)
		dirent = PingDirent()
		dirent.name = node.name
		dirent.inode = node.inode
		self.entries.append(dirent)
		node.parent = self

	def del_node(self,name,node=None):
		self.entries = [x for x in self.entries if x.name != name]
		if node: node.parent = None

	def get_dirent(self,name,node=None):
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
			self.cache = PingDirectory('/') # create root
			self.add(self.cache,0) # and cache it

		except:
			print 'General Exception'
			from traceback import print_exc
			print_exc()

	def read_inode(self,inode,length=0):
		log.debug('PingFS::read_inode: inode=%d length=%d'%(inode,length))
		if length == 0: block_size = max(self.disk.block_size(),PingFile.file_header)
		data = self.disk.read(inode,block_size)
		size = PingFile.file_header + interpretSize(data)

		if size > len(data):
			data = self.disk.read(inode,size)
		return data

	def read_as_file(self,inode):
		log.debug('PingFS::read_as_file: inode=%d'%inode)
		data = self.read_inode(inode)
		pfile = make
		pfile = makePingFile(data)
		return pfile

	def read_as_dir(self,inode):
		log.debug('PingFS::read_as_dir: inode=%d'%inode)
		data = self.read_inode(inode)
		pdir = makePingDirectory(data)
		if not (pdir.type & stat.S_IFDIR):
			raise Exception('read_as_dir: %s (%d,%d) -> %x %d'%(pdir.name,inode,len(data),pdir.type,len(pdir.entries)))
		return pdir

	def cache_hit(self,name,pFile=None):
		if not self.cache: return False
		if self.cache.name != name: return False
		if pFile and self.cache.inode != pFile.inode: return False
		return True

	def get(self, path):
		log.notice('PingFS::get %s'%path)
		if self.cache_hit(path): return self.cache
		if path == '/' or path == '':
			if self.cache.inode == 0: return self.cache
			return self.read_as_dir(0)
		parts = path.rsplit('/',1)
		if len(parts) != 2: raise Exception('PingFS::get_file: invalid path: %s'%path)
		rPath,fName = parts[0],parts[1]
		pDir = self.get(rPath)
		if pDir and pDir.type == stat.S_IFDIR:
			pEntry = pDir.get_dirent(fName)
			self.cache = pDir # cache the directory
			self.cache.name = rPath
			if pEntry:
				data = self.read_inode(pEntry.inode)
				pFile = interpretFile(data)
				pFile.name = pEntry.name
				return pFile
		return None

	def get_both(self, path):
		log.notice('PingFS::get_both %s'%path)
		if self.cache_hit(path):
			if self.cache.parent:
				return (self.cache.parent,self.cache)
		if path == '/' or path == '':
			if self.cache.inode == 0:
				return (self.cache,self.cache)
			return self.read_as_dir(0)
		parts = path.rsplit('/',1)
		if len(parts) != 2: raise Exception('PingFS::get_both: invalid path: %s'%path)
		sPath,sName = parts[0],parts[1]
		pDir = self.get(sPath)
		if not pDir: return (None,None)
		if not pDir.type == stat.S_IFDIR: return (None,None)
		pEntry = pDir.get_dirent(sName)
		self.cache = pDir # cache the directory
		self.cache.name = sPath
		if not pEntry: return (pDir,None)
		data = self.read_inode(pEntry.inode)
		pFile = interpretFile(data)
		pFile.name = pEntry.name
		return (pDir,pFile)

	def get_parent(self, path, pFile=None):
		if path == '/' or path == '': return self.read_as_dir(0)
		parts = path.rsplit('/',1)
		if len(parts) != 2:
			log.exception('PingFS::get_parent: invalid path: %s'%path)
			return None
		pDir = self.get(parts[0])
		if pDir.type != stat.S_IFDIR: return None
		return pDir

	def root_node(self, node):
		if node.inode == 0: return True
		return False

	def unlink(self, path, pFile=None, pDir=None):
		log.notice('PingFS::unlink %s'%path)
		if not pFile:             pFile = self.get(path)
		if not pFile:             return False
		if self.root_node(pFile): return False # don't delete the root
		if not pDir:              pDir = self.get_parent(path,pFile)
		if pDir:  self.disconnect(path,pFile,pDir)
		self.delete(path,pFile)
		return True

	def disconnect(self, path, pFile=None, pDir=None):
		log.notice('PingFS::disconnect %s'%path)
		if path == '/' or path == '': return False
		if not pFile: pFile = self.get(path)
		if not pFile: return False
		if not pDir:  pDir = self.get_parent(path,pFile)
		if not pDir:  return True # we're technically disconnected
		pDir.del_node(pFile.name,pFile)
		self.update(pDir)
		return True

	def delete(self, path, pFile=None): # assumes node disconnected from dir tree
		log.notice('PingFS::delete %s'%path)
		if not pFile: pFile = self.get(path)
		if not pFile: return False
		if self.cache_hit(path,pFile): self.cache = None
		self.disk.delete(pFile.inode,pFile.size())

	def move_blocks(self, path, pFile, dest, pDir=None):
		log.debug('move_blocks: %s (%d->%d)'%(pFile.name,pFile.inode,dest))
		if self.root_node(pFile): return False # don't move the root
		if not pDir: pDir = self.get_parent(path,pFile)
		if not pDir: return False
		self.delete(pFile.name,pFile)
		self.add(pFile,dest)
		dirent = pDir.get_dirent(pFile.name,pFile)
		dirent.inode = dest
		self.update(pDir)
		return True

	def move_links(self, pFile, oDir, nDir):
		log.notice('move_links: %s (%s -> %s)'%(pFile.name,oDir.name,nDir.name))
		if self.root_node(pFile): raise Exception('move_link on root!')
		if not oDir.get_dirent(pFile.name,pFile): return False
		oDir.del_node(pFile.name,pFile); self.update(oDir)
		nDir.add_node(pFile.name,pFile); self.update(nDir)
		return True

	def cache_update(self,node):
		if not self.cache: return
		if self.cache.inode != node.inode: return
		self.cache = node

	def add(self,node,force_inode=None):
		if force_inode != None:
			node.inode = force_inode
		else:
			node.inode = self.disk.get_region(node.size())
			if not node.inode: return None
		log.notice('PingFS::add %s at %d'%(node.name,node.inode))
		self.disk.write(node.inode,node.serialize())
		self.cache_update(node)
		return node.inode

	def relocate(self,pFile,pDir=None):
		log.notice('relocating %s to larger region'%pFile)
		region = self.disk.get_region(pFile.size())
		if not region: raise Exception('PingFS::update %s at %d: collision correction fail'%(pFile.name,pFile.inode))
		if not pFile.parent: pFile.parent = pDir
		if not pFile.parent: raise Exception('PingFS::update %s at %d: collision parent not found'%(pFile.name,pFile.inode))
		if not self.move_blocks(None,pFile,region,pFile.parent):
			raise Exception('PingFS::update %s at %d: collision correction failed'%(pFile.name,pFile.inode))
		log.notice('relocated %d:%s to region %d'%(pFile.inode,pFile.name,region))
		return True
	
	def update(self,pFile,pDir=None):
		log.debug('PingFS::update %s at %d [%d -> %d]'%(pFile.name,pFile.inode,pFile.disk_size,pFile.size()))
		if pFile.size() > pFile.disk_size:
			region = self.disk.test_region(pFile.inode,pFile.disk_size,pFile.size())
			if region != pFile.inode: return self.relocate(pFile,pDir) # continuing would cause collision
		self.disk.write(pFile.inode,pFile.serialize())
		self.cache_update(pFile)
		return True

	def create(self,path,buf='',offset=0):
		log.debug('PingFS::create %s (offset=%d len=%d)'%(path,offset,len(buf)))
		parts = path.rsplit('/',1)
		if len(parts) != 2:
			log.exception('PingFS::create: invalid path: %s'%path)
			return False
		rPath,rName = parts[0],parts[1]
		pDir = self.get(rPath)
		if not pDir:
			log.error('PingFS::create invalid parent dir: %s'%path)
			return False
		pFile = PingFile(rName)
		if not offset: offset = ''
		else: offset = '\0'*offset
		pFile.data = offset + buf
		inode = self.add(pFile)
		pDir.add_node(pFile)
		self.update(pDir)
		return pFile
		
	def stop(self):
		log.info('PingFS: stopping')
		self.disk.stop()

def init_fs(FS):
	log.notice('building nodes')
	d1 = PingDirectory('/')
	d2 = PingDirectory('l1')
	f1 = PingFile('apples')
	f2 = PingFile('banana')
	f1.mode = 0700
	f1.uid = 1000
	f1.gid = 1000

	log.notice('adding nodes to system')
	FS.add(d1,0)
	FS.add(d2)
	FS.add(f1)
	FS.add(f2)

	log.notice('connecting nodes')
	d1.add_node(d2)
	d1.add_node(f1)
	d2.add_node(f2)

	log.notice('fleshing out nodes')
	f1.data = 'delicious apples\n'
	f2.data = 'ripe yellow bananas\n'

	log.notice('updating nodes in system')
	FS.update(d1)
	FS.update(d2)
	FS.update(f1)
	FS.update(f2)

	FS.create('/l1/bonus','contenttttttt',0)

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
		server = ping.select_server(log)
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
