import threading, logging, time, locale

logging.TRACE = 5
logging.NOTICE = 15
logging.addLevelName(logging.TRACE, 'TRACE')
logging.addLevelName(logging.NOTICE, 'NOTICE')

def setup_log(name,level=logging.ERROR):
	log = logging.getLogger(name)
	setattr(log.__class__, 'notice', log_notice)
	setattr(log.__class__, 'trace', log_trace)
	log.setLevel(level)
	return log

def start_log(logger,level=logging.NOTICE):
	logger.setLevel(level)
	addStreamHandler(logger)
	addFileHandler(logger)

def addFileHandler(log,level=None):
	formatter = logging.Formatter('[%(levelname)s] %(message)s')
	handler = logging.FileHandler(log.name+'.log')
	if level: handler.setLevel(level)
	handler.setFormatter(formatter)
	log.addHandler(handler)

def addStreamHandler(log,level=None):
	formatter = logging.Formatter('[%(levelname)s] %(message)s')
	handler = logging.StreamHandler()
	if level: handler.setLevel(level)
	handler.setFormatter(formatter)
	log.addHandler(handler)

def log_generic(self, level, msg, *args, **kwargs):
	if self.manager.disable >= level: return
	if level >= self.getEffectiveLevel():
		apply(self._log, (level,msg,args), kwargs)

def log_notice(self, msg, *args, **kwargs):
	log_generic(self,logging.NOTICE,msg,*args,**kwargs)

def log_trace(self, msg, *args, **kwargs):
	log_generic(self,logging.TRACE,msg,*args,**kwargs)

def enableAllLogs(level=logging.INFO):
	import ping, ping_disk, ping_server
	start_log(ping.log,level)
	start_log(ping_disk.log,level)
	start_log(ping_server.log,level)

	import ping_filesystem, ping_fuse
	start_log(ping_fuse.log,level)
	start_log(ping_filesystem.log,level)

import ping
def humanize_bytes(bytes, precision=2):
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
	if bytes == 1: return '1 byte'
	for factor, suffix in abbrevs:
		if bytes >= factor: break
	return '%.*f %s' % (precision, float(bytes) / factor, suffix)

class PingReporter(threading.Thread):
	def __init__(self, log, server, interval=90):
		locale.setlocale(locale.LC_ALL,'')
		threading.Thread.__init__(self)
		self.interval = interval
		self.server = server
		self.running = 1
		self.log = log

	def stop(self):
		log.info('reporter terminated')
		self.running = 0

	def run(self):
		start = time.time()
		self.log.info('reporter started at %s'%time.ctime())
		while self.running:
			time.sleep(self.interval)
			bw = humanize_bytes(ping.ping_bandwidth)
			num = locale.format('%d', ping.ping_count, True)
			tstr = time.strftime('%H:%M:%S',time.gmtime(time.time()-start))
			self.log.info('%s (%s pings) -> %s elapsed'%(bw,num,tstr))
