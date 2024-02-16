import common
import settings
import time
import threading
import logging


logger = logging.getLogger("cbt")


class ScrubTestThreadBackground(threading.Thread):
    def __init__(self, config, cluster, callback, stoprequest, haltrequest, startiorequest):
        threading.Thread.__init__(self)
        self.config = config
        self.cluster = cluster
        self.callback = callback
        self.state = 'pre'
        self.states = {'pre': self.pre, 'fill_scrub_pool': self.fill_scrub_pool, 'start_scrub':self.start_scrub,
                       'post': self.post, 'done': self.done}
        self.startiorequest = startiorequest
        self.stoprequest = stoprequest
        self.haltrequest = haltrequest
        self.outhealthtries = 0
        self.inhealthtries = 0
        self.maxhealthtries = 60
        self.health_checklist = ["peering", "recovery_wait", "stuck", "inactive", "unclean", "recovery"]
        self.ceph_cmd = self.cluster.ceph_cmd
        self.lasttime = time.time()

    def logcmd(self, message):
        return 'echo "[`date`] %s" >> %s/scrub.log' % (message, self.config.get('run_dir'))

    def pre(self):
        pre_time = self.config.get("pre_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Starting Scrub Test Thread, waiting %s seconds.' % pre_time)).communicate()
        time.sleep(pre_time)
        self.state = 'fill_scrub_pool'

    def fill_scrub_pool(self):
        scrub_log = "%s/scrub.log" % self.config.get('run_dir')
        scrub_stats_log = "%s/scrub_stats.log" % self.config.get('run_dir')
        ret = self.cluster.check_health(self.health_checklist, None, None)

        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret)).communicate()

        self.cluster.maybe_populate_scrub_pool()
        common.pdsh(settings.getnodes('head'), self.logcmd("osdout state - Sleeping for 10 secs after populating scrub pool.")).communicate()
        time.sleep(10)
        self.lasttime = time.time()
        self.state = "start_scrub"

    def start_scrub(self):
        scrub_stats_log = "%s/scrub_stats.log" % self.config.get('run_dir')
        self.startiorequest.set()
        self.cluster.initiate_scrub()
        ret = self.cluster.check_scrub(scrub_stats_log)
        if ret == 1:
            self.state = "post"

    def post(self):
        if self.stoprequest.isSet():
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, but stoprequest is set, finishing now.')).communicate()
            self.haltrequest.set()
            return

        if self.config.get("repeat", False):
            # reset counters
            self.outhealthtries = 0
            self.inhealthtries = 0

            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, but repeat is set.  Moving to "osdout" state.')).communicate()
            self.state = "fill_scrub_pool"
            return

        common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, finishing up...')).communicate()
        self.state = "done"

    def done(self):
        common.pdsh(settings.getnodes('head'), self.logcmd("Done.  Calling parent callback function.")).communicate()
        self.callback()
        self.haltrequest.set()

    def join(self, timeout=None):
        common.pdsh(settings.getnodes('head'), self.logcmd('Received notification that parent is finished and waiting.')).communicate()
        super(ScrubTestThreadBackground, self).join(timeout)

    def run(self):
        self.haltrequest.clear()
        self.stoprequest.clear()
        self.startiorequest.clear()
        while not self.haltrequest.isSet():
          self.states[self.state]()
        common.pdsh(settings.getnodes('head'), self.logcmd('Exiting scrub test thread.  Last state was: %s' % self.state)).communicate()


class ScrubRecoveryThreadBackground(threading.Thread):
    def __init__(self, config, cluster, callback, stoprequest, haltrequest, startiorequest):
        threading.Thread.__init__(self)
        self.config = config
        self.cluster = cluster
        self.callback = callback
        self.state = 'pre'
        self.states = {'pre': self.pre, 'markdown': self.markdown, 'fill_pools': self.fill_pools, 
                       'start_recovery_and_scrub':self.start_recovery_and_scrub, 'post': self.post, 'done': self.done}
        self.startiorequest = startiorequest
        self.stoprequest = stoprequest
        self.haltrequest = haltrequest
        self.outhealthtries = 0
        self.inhealthtries = 0
        self.maxhealthtries = 60
        self.health_checklist = ["peering", "recovery_wait", "stuck", "inactive", "unclean", "recovery"]
        self.ceph_cmd = self.cluster.ceph_cmd
        self.lasttime = time.time()

    def logcmd(self, message):
        return 'echo "[`date`] %s" >> %s/scrub_recov.log' % (message, self.config.get('run_dir'))

    def pre(self):
        pre_time = self.config.get("pre_time", 60)
        common.pdsh(settings.getnodes('head'), self.logcmd('Starting Scrub+Recovery Test Thread, waiting %s seconds.' % pre_time)).communicate()
        time.sleep(pre_time)
        lcmd = self.logcmd("Setting the ceph osd noup flag")
        common.pdsh(settings.getnodes('head'), '%s -c %s osd set noup;%s' % (self.ceph_cmd, self.cluster.tmp_conf, lcmd)).communicate()
        self.state = 'markdown'

    def markdown(self):
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s down." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd down %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s out." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd out %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
        common.pdsh(settings.getnodes('head'), self.logcmd('Waiting for the cluster to break and heal')).communicate()
        self.lasttime = time.time()
        self.state = 'fill_pools'


    def fill_pools(self):
        reclog = "%s/recovery.log" % self.config.get('run_dir')
        recstatslog = "%s/recovery_stats.log" % self.config.get('run_dir')
        ret = self.cluster.check_health(self.health_checklist, reclog, recstatslog)

        common.pdsh(settings.getnodes('head'), self.logcmd("ret: %s" % ret)).communicate()

        if ret == 0:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster never went unhealthy.')).communicate()
        else:
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster appears to have healed.')).communicate()
            rectime = str(time.time() - self.lasttime)
            common.pdsh(settings.getnodes('head'), 'echo Time: %s >> %s' % (rectime, recstatslog)).communicate()
            common.pdsh(settings.getnodes('head'), self.logcmd('Time: %s' % rectime)).communicate()

        # Populate the recovery pool
        self.cluster.maybe_populate_recovery_pool()

        common.pdsh(settings.getnodes('head'), self.logcmd("osdout state - Sleeping for 10 secs after populating recovery pool.")).communicate()
        time.sleep(10)
        lcmd = self.logcmd("Unsetting the ceph osd noup flag")
        self.cluster.disable_recovery()
        common.pdsh(settings.getnodes('head'), '%s -c %s osd unset noup;%s' % (self.ceph_cmd, self.cluster.tmp_conf, lcmd)).communicate()
        for osdnum in self.config.get('osds'):
            lcmd = self.logcmd("Marking OSD %s up." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd up %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
            lcmd = self.logcmd("Marking OSD %s in." % osdnum)
            common.pdsh(settings.getnodes('head'), '%s -c %s osd in %s;%s' % (self.ceph_cmd, self.cluster.tmp_conf, osdnum, lcmd)).communicate()
        self.lasttime = time.time()
        # Populate the scrub pool
        logger.info("Sleep before scrub populate")
        time.sleep(10)
        self.cluster.maybe_populate_scrub_pool()
        self.state = "start_recovery_and_scrub"
    

    def start_recovery_and_scrub(self):
        self.startiorequest.set()
        self.cluster.initiate_scrub()
        self.cluster.enable_recovery()
        recstatslog = "%s/recovery_backfill_stats.log" % self.config.get('run_dir')
        scrub_stats_log = "%s/scrub_stats.log" % self.config.get('run_dir')
        backfill = threading.Thread(target=self.cluster.check_backfill, args=(self.health_checklist, "%s/recovery.log" % self.config.get('run_dir'), recstatslog,))
        scrub_check = threading.Thread(target=self.cluster.check_scrub, args=(scrub_stats_log,))
        backfill.start()
        scrub_check.start()
        backfill.join()
        scrub_check.join()
        self.state = "post"


    def post(self):
        if self.stoprequest.isSet():
            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, but stoprequest is set, finishing now.')).communicate()
            self.haltrequest.set()
            return

        if self.config.get("repeat", False):
            # reset counters
            self.outhealthtries = 0
            self.inhealthtries = 0

            common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, but repeat is set.  Moving to "markdown" state.')).communicate()
            self.state = "markdown"
            return

        common.pdsh(settings.getnodes('head'), self.logcmd('Cluster is healthy, finishing up...')).communicate()
        self.state = "done"

    def done(self):
        common.pdsh(settings.getnodes('head'), self.logcmd("Done.  Calling parent callback function.")).communicate()
        self.callback()
        self.haltrequest.set()

    def join(self, timeout=None):
        common.pdsh(settings.getnodes('head'), self.logcmd('Received notification that parent is finished and waiting.')).communicate()
        super(ScrubRecoveryThreadBackground, self).join(timeout)

    def run(self):
        self.haltrequest.clear()
        self.stoprequest.clear()
        self.startiorequest.clear()
        while not self.haltrequest.isSet():
          self.states[self.state]()
        common.pdsh(settings.getnodes('head'), self.logcmd('Exiting scrub+recovery test thread.  Last state was: %s' % self.state)).communicate()
