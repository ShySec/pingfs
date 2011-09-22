#!/usr/bin/python

import ping
import fuse, sys

from time import time

import stat    # for file properties
import os      # for filesystem modes (O_RDONLY, etc)
import errno   # for error number codes (ENOENT, etc)
               # - note: these must be returned as negatives

import ping_reporter
import ping_filesystem
import logging
import posix
import time

fuse.fuse_python_api = (0,2)

log = logging.getLogger('PingFuse')
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
handler = logging.FileHandler('PingFuse.log')
handler.setFormatter(formatter)
log.addHandler(handler)
handler = logging.StreamHandler()
handler.setFormatter(formatter)
log.addHandler(handler)

class PingFuse(fuse.Fuse):
	def __init__(self, server):
		self.FS = ping_filesystem.PingFS(server)
		#ping.drop_privileges()
		fuse.Fuse.__init__(self)
		log.info('init complete')

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
		log.info('readdir (%s)'%path)
		pDir = self.FS.get(path)
		if not pDir: return -errno.ENOENT
		if pDir.type != stat.S_IFDIR:
			return [fuse.Direntry(pDir.name)]

		files = [fuse.Direntry('.'),fuse.Direntry('..')]
		for e in pDir.entries:
			files.append(fuse.Direntry(e.name))
		return files

	def mkdir(self, path, mode ):
		log.info('mkdir (%s,%04o)'%(path,mode))
		if path == '/' or path == '': return -errno.EACCESS
		rPath,rName = path.rsplit('/',1)
		pDir = self.FS.get(rPath)
		if not pDir: return -errno.ENOENT
		if pDir.get_dirent(rName): return -errno.EEXIST

		nDir = ping_filesystem.PingDirectory(6*1024,rName)
		pDir.add_node(nDir)
		self.FS.add(pDir)
		self.FS.add(nDir)
		return 0

	def open ( self, path, flags ):
		log.info('open (%s,%x)'%(path,flags))
		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT
		return 0

	def read ( self, path, length, offset ):
		log.info('read (%s,%d,%d)'%(path,length,offset))
		pFile = self.FS.get(path)
		if not pFile: return -errno.ENOENT
		if offset > len(pFile.data): return -errno.EINVAL
		if pFile.type == stat.S_IFDIR: return -errno.EISDIR
		return pFile.data[offset:offset+length]

	def rename(self, oldPath, newPath):
		log.info('rename (%s,%s)'%(oldPath,newPath))
		return -errno.ENOSYS

	def mythread ( self ):
		log.info('mythread')
		return -errno.ENOSYS

	def chmod ( self, path, mode ):
		log.info('chmod (%s,%04o)'%(path,mode))
		return -errno.ENOSYS

	def chown ( self, path, uid, gid ):
		log.info('chown (%s,%d,%d)'%(path,uid,gid))
		return -errno.ENOSYS

	def fsync ( self, path, isFsyncFile ):
		log.info('fsync (%s,%d)'%(path,isFsyncFile))
		return -errno.ENOSYS

	def link ( self, targetPath, linkPath ):
		log.info('link (%s,%s)'%(targetPath, linkPath))
		return -errno.ENOSYS

	def mknod ( self, path, mode, dev ):
		log.info('mknod (%s,%04o,%d)'%(path,mode,dev))
		return -errno.ENOSYS

	def readlink ( self, path ):
		log.info('readlink (%s)'%path)
		return -errno.ENOSYS

	def release ( self, path, flags ):
		log.info('release (%s,%x)'%(path,flags))
		return -errno.ENOSYS

	def rmdir ( self, path ):
		log.info('rmdir (%s)'%path)
		return -errno.ENOSYS

	def statfs ( self ):
		log.info('statfs')
		return -errno.ENOSYS

	def symlink ( self, targetPath, linkPath ):
		log.info('symlink (%s,%s)'%(targetPath, linkPath))
		return -errno.ENOSYS

	def truncate ( self, path, size ):
		log.info('truncate (%s,%d)'%(path, size))
		return -errno.ENOSYS

	def unlink ( self, path ):
		log.info('unlink (%s)'%path)
		return -errno.ENOSYS

	def utime ( self, path, times ):
		log.info('utime (%s,%d)'%(path,times))
		return -errno.ENOSYS

	def write ( self, path, buf, offset ):
		log.info('write (%s,%d,%d): %s'%(path,len(buf),offset,data))
		return -errno.ENOSYS



if __name__ == "__main__":
	server = ping.select_server()
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
		

