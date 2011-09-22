import threading, logging, time
import ping

class PingReporter(threading.Thread):
	def __init__(self, log, server, interval=90):
		threading.Thread.__init__(self)
		self.interval = interval
		self.server = server
		self.running = 1
		self.log = log

	def stop(self):
		log.info('ping::reporter terminated')
		self.running = 0

	def run(self):
		start = time.time()
		self.log.info('ping::reporter starting at %s'%time.ctime())
		while self.running:
			time.sleep(self.interval)
			bw = ping.humanize_bytes(ping.ping_bandwidth)
			tstr = time.strftime('%H:%M:%S',time.gmtime(time.time()-start))
			self.log.info('%s (%d pings) -> %s elapsed'%(bw,ping.ping_count,tstr))
