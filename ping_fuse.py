#!/usr/bin/python

import os, sys, stat, errno, posix, logging, time, fuse
import ping, ping_reporter, ping_filesystem
from time import time

fuse.fuse_python_api = (0,2)

log = ping_reporter.setup_log('PingFuse')

class PingFuse(fuse.Fuse):
	def __init__(self, server):
		self.FS = ping_filesystem.PingFS(server)
		#ping.drop_privileges()
		fuse.Fuse.__init__(self)
		log.notice('ping::fuse: initialized (%d-byte blocks)'%self.FS.disk.block_size())

	def fsinit(self):
		self.reporter = ping_reporter.PingReporter(log,'',90)
		self.reporter.start()

	def getattr(self, path):
		"""
		- st_mode (protection bits)
		- st_ino (inode number)
		- st_dev (device)
		- st_nlink (number of hard links)
		- st_uid (user ID of owner)
		- st_gid (group ID of owner)
		- st_size (size of file, in bytes)
		- st_atime (time of most recent access)
		- st_mtime (time of most recent content modification)
		- st_ctime (platform dependent; time of most recent metadata change on Unix,
				    or the time of creation on Windows).
		"""

		log.info('getattr: %s' % path)

		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT

		st = fuse.Stat()
		st.st_mode = pFile.type | pFile.mode
		st.st_ino = pFile.inode
		st.st_nlink = pFile.links()
		st.st_uid = 1000 #pFile.uid
		st.st_gid = 1000 #pFile.gid
		st.st_size = pFile.size()
		#st.st_atime = time()
		#st.st_mtime = time()
		#st.st_ctime = time()
		#st.st_dev = 2050L
		return st

	def readdir(self, path, offset):
		log.info('readdir: %s'%path)
		pDir = self.FS.get(path)
		if not pDir: return -errno.ENOENT
		if pDir.type != stat.S_IFDIR:
			return [fuse.Direntry(pDir.name)]

		files = [fuse.Direntry('.'),fuse.Direntry('..')]
		for e in pDir.entries:
			files.append(fuse.Direntry(e.name))
		return files

	def mkdir(self, path, mode):
		log.info('mkdir: %s mode=%04o'%(path,mode))
		if path == '/' or path == '': return -errno.EACCESS
		if self.FS.get(path): return -errno.EEXIST
		rPath,rName = path.rsplit('/',1)
		pDir = self.FS.get(rPath)
		if not pDir: return -errno.ENOENT

		nDir = ping_filesystem.PingDirectory(rName)
		self.FS.add(nDir) # acquire inode
		pDir.add_node(nDir) # add dirent
		self.FS.update(pDir) # save
		return 0

	def open(self, path, flags):
		log.info('open: %s flags=%x'%(path,flags))
		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT
		return 0

	def read(self, path, length, offset):
		log.info('read: %s region=%d,%d'%(path,offset,length))
		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT
		if offset > len(pFile.data): return -errno.EINVAL
		if pFile.type == stat.S_IFDIR: return -errno.EISDIR
		return pFile.data[offset:offset+length]

	def chmod(self, path, mode):
		log.info('chmod: %s mode=%04o'%(path,mode))
		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT
		pFile.mode = mode
		self.FS.update(pFile)
		return 0

	def chown(self, path, uid, gid):
		log.info('chown: %s uid=%d gid=%d)'%(path,uid,gid))
		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT
		pFile.uid = uid
		pFile.gid = gid
		self.FS.update(pFile)
		return 0

	def rmdir(self, path):
		log.info('rmdir: %s'%path)
		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT
		if pFile.type != stat.S_IFDIR: return -errno.ENOTDIR
		if self.FS.unlink(path,pFile): return 0
		return -errno.EINVAL

	def unlink(self, path):
		log.info('unlink: %s'%path)
		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT
		if pFile.type != stat.S_IFREG: return -errno.ENOTDIR
		if self.FS.unlink(path,pFile): return 0
		return -errno.EINVAL

	def write(self, path, buf, offset):
		log.info('write: %s region=%d,%d'%(path,offset,len(buf)))
		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT
		pDir = self.FS.get_parent(path,pFile)
		if not pDir: raise Exception('write failed to find parent after filding child!')

	#		pFile = self.FS.create(path, buf, offset)
	#		if not pFile: return -errno.EINVAL
	#	else:
		if not offset: offset = ''
		else: offset = '\0'*offset
		pFile.data = offset + buf
		if not self.FS.update(pFile,pDir):
			return -errno.EINVAL
		return len(buf)

	def truncate(self, path, size):
		log.info('truncate: %s size=%d'%(path, size))
		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT
		if size > len(pFile.data): return -errno.EINVAL
		if pFile.type != stat.S_IFREG: return -errno.EINVAL
		pFile.data = pFile.data[:size]
		self.FS.update(pFile)
		return 0

	def mknod(self, path, mode, dev):
		log.info('mknod: %s mode=%04o dev=%d)'%(path,mode,dev))
		if not mode & stat.S_IFREG: return -errno.ENOSYS
		pFile = self.FS.get(path)
		if pFile: return -errno.EEXIST
		pFile = self.FS.create(path)
		if not pFile: return -errno.EINVAL
		pFile.mode = mode & 0777
		self.FS.add(pFile)
		return 0

	def link(self, targetPath, linkPath):
		log.info('link: %s <- %s)'%(targetPath, linkPath))
		return -errno.ENOSYS

	def readlink(self, path):
		log.info('readlink: %s'%path)
		return -errno.ENOSYS

	def symlink(self, targetPath, linkPath):
		log.info('symlink: %s <- %s'%(targetPath, linkPath))
		return -errno.ENOSYS

	def rename(self, oldPath, newPath):
		log.info('rename: %s -> %s'%(oldPath,newPath))
		return -errno.ENOSYS

#	def mythread ( self ):
#		log.info('mythread')
#		return -errno.ENOSYS

	def release(self, path, flags):
		log.info('release: %s flags=%x'%(path,flags))
		return -errno.ENOSYS

	def statf(self):
		log.info('statfs')
		return -errno.ENOSYS

	def utime(self, path, times):
		log.info('utime: %s times=%s'%(path,times))
		return -errno.ENOSYS

	def fsync(self, path, isFsyncFile):
		log.info('fsync: %s fsyncFile? %s'%(path,isFsyncFile))
		return -errno.ENOSYS



if __name__ == "__main__":
	#ping_reporter.enableAllLogs(logging.TRACE)
	ping_reporter.start_log(log,logging.DEBUG)
	ping_reporter.start_log(ping_filesystem.log,logging.ERROR)
	server = ping.select_server(log)
	if len(sys.argv) < 2:
		print 'usage: %s <mountpoint>' % sys.argv[0]
		sys.exit(1)
	sys.argv.append('-f')
	fs = PingFuse(server)
	#fs.parser.add_option(mountopt="root",metavar="PATH", default='/')
	#fs.parse(values=fs, errex=1)
	fs.parse(errex=1)

	fs.flags = 0
	#fs.multithreaded = 0
	ping_filesystem.init_fs(fs.FS)
	#ping_filesystem.test_fs(fs.FS)

	log.info('file system up and running')
	try:
		fs.main()
	except KeyboardInterrupt:
		log.info('fs stopping')
		sys.exit(1)
		

