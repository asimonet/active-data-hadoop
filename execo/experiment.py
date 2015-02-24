#!/usr/bin/env python

import traceback
import logging, time, datetime, signal
import pprint, os, sys, math
pp = pprint.PrettyPrinter(indent=4).pprint

import execo as EX
from string import Template
from execo import configuration
from execo.log import style
from execo.process import ProcessOutputHandler
import execo_g5k as EX5
from execo_g5k.api_utils import get_cluster_site
from execo_engine import Engine, ParamSweeper, logger, sweep, sweep_stats, slugify
from hadoop_g5k import HadoopCluster, HadoopJarJob
#EX.logger.setLevel(logging.ERROR)
#logger.setLevel(logging.ERROR)

#EXCLUDED_ELEMENTS = ['paranoia-4', 'paranoia-7', 'paranoia-8']
EXCLUDED_ELEMENTS = []

# Shortcut
funk = EX5.planning

job_name = 'ActiveDataHadoop'
job_path = "/root/hduser/terasort-"

default_ad_cluster        = 'parapide'
default_work_cluster    = 'paranoia'
default_n_nodes            = 5
default_walltime        = "3:00:00"

XP_BASEDIR = '/home/ansimonet/active_data_hadoop'
AD_JAR_NAME = 'active-data-lib-0.2.0.jar'
HADOOP_LOG_PATH = '/tmp/hadoop/logs/'

class ad_hadoop(Engine):

    def __init__(self):
        """Define options for the experiment"""
        super(ad_hadoop, self).__init__()
        self.options_parser.add_option("-k", dest="keep_alive",
                                        help="Keep the reservation alive.",
                                        action="store_true")
        self.options_parser.add_option("-j", dest="job_id",
                                        help="job_id to relaunch an engine.",
                                        type=int)
        self.options_parser.add_option("--ad-cluster",
                                       default=default_ad_cluster,
                                       help="The cluster on which to run ActiveData.")
        self.options_parser.add_option("--work-cluster",
                                       default=default_work_cluster,
                                       help="The cluster on which to run Hadoop.")
        self.options_parser.add_option("--n-nodes",
                                       default=default_n_nodes,
                                       help="The number of nodes to use.")
        self.options_parser.add_option("--n-reducers",
                                       help="The number of reducers to use.")
        self.options_parser.add_option("--walltime",
                                       default=default_walltime,
                                       help="The duration of the experiment.")
        self.options_parser.add_option("--size", type=int,
                                       help="Dataset size (in GB)")

    def run(self):
        """Perform experiment"""
        logger.detail(self.options)
        try:
            # Checking the options
            if self.options.n_reducers is None or self.options.size is None:
                logger.error("Both --n-reducer and --size are mandatory.")
                exit(1)
            
            # Retrieving the hosts for the experiment
            hosts = self.get_hosts()
            # Deploying OS and copying required file
            AD_node, hadoop_cluster = self.setup_hosts(hosts)
            
            # Starting the processes
            server = self._start_active_data_server(AD_node)
            clients = self._start_active_data_clients(AD_node, hadoop_cluster)
            listener = self._start_active_data_listener(AD_node)
            data_size = self.options.size * 1024 ** 3
            
            # First Hadoop job to generate the dataset
            hadoop_stdout_path = os.path.join(self.result_dir, "hadoop.stdout")
            hadoop_stderr_path = os.path.join(self.result_dir, "hadoop.stderr")
            fout = open(hadoop_stdout_path, 'w')
            ferr = open(hadoop_stderr_path, 'w')

            logger.info("Generating a %s input file" % (sizeof_fmt(data_size)))
            generate = HadoopJarJob('jars/hadoop-examples-1.2.1.jar',
                               "teragen %s %sinput" % (data_size, job_path))
            gen_out, gen_err = hadoop_cluster.execute_job(generate)
            fout.write(gen_out)
            ferr.write(gen_err)
            
            # Second Hadoop job: the actual benchmark
            sort = HadoopJarJob('jars/hadoop-examples-1.2.1.jar',
                               "terasort -Dmapred.reduce.tasks=%s %sinput "
                               "%soutput" % (self.options.n_reducers, job_path,
                                             job_path))
            sort_out, sort_err = hadoop_cluster.execute_job(sort)
            fout.write(sort_out)
            ferr.write(sort_err)
            
            fout.close()
            ferr.close()
            
            # Terminate the scrappers and save their output
            clients.kill()
            for p in clients.processes:
                f = open(os.path.join(self.result_dir, 'scrapper-' + p.host.address + '.out'))
                f.write(p.stdout)
                f.close()
            
            # Terminate everything else
            listener.kill()
            server.kill()
            
            # Get the logs back from the machines
            self._get_logs(AD_node, hadoop_cluster)

        finally:
            if not self.options.keep_alive:
                EX5.oardel([(self.job_id, self.site)])
        exit()

    def _start_active_data_clients(self, server, hadoop_cluster, port=1200,
                                   hdf_path='/root/hduser/terasort-input'):
        """Return a started client action"""
        cmd = "java -cp 'jars/*' " + \
            "org.inria.activedata.hadoop.HadoopScrapper " +\
            server + " " + HADOOP_LOG_PATH + "hadoop-root-tasktracker-{{{host}}}.log"
        tasktracker = EX.Remote(cmd, hadoop_cluster.hosts)
        cmd = "java -cp 'jars/*' " + \
            "org.inria.activedata.hadoop.HadoopScrapper " +\
            server + " " + HADOOP_LOG_PATH + "hadoop-root-jobtracker-{{{host}}}.log"
        jobtracker = EX.Remote(cmd, [hadoop_cluster.master])

        clients = EX.ParallelActions([tasktracker, jobtracker])
        clients.start()

        return clients

    def _start_active_data_listener(self, ad_server, port=1200,
                                   hdf_path='/root/hduser/terasort-input'):
        """Return a started listener process"""
        cmd = "java -cp 'jars/*' org.inria.activedata.hadoop.HadoopListener %s %s %s" \
                % (ad_server, port, hdf_path)
        out_path = os.path.join(self.result_dir, "listener.out")
        listener = EX.SshProcess(cmd, ad_server)
        listener.stdout_handlers.append(out_path)
        listener.stderr_handlers.append(out_path)
        listener.start()
        return listener

    def _start_active_data_server(self, ad_server):
        """Return a started server process"""
        out_path = os.path.join(self.result_dir, "server.out")
        cmd = "java -Djava.security.policy=server.policy -jar jars/%s" % AD_JAR_NAME
        logger.info("Running command " + cmd)
        server = EX.SshProcess(cmd, ad_server)
        server.stdout_handlers.append(out_path)
        server.stderr_handlers.append(out_path)
        server.start()
        logger.info("Active Data Service started on " + server.host.address)
        time.sleep(2)

        if not server.running:
            logger.error("Active Data Service crashed\n %s \n%s",
                        server.stdout, server.stderr)
            return False

        return server

    def setup_hosts(self, hosts):
        """Deploy operating setup active data on the service node and
        Hadoop on all"""
        logger.info('Deploying hosts')
        deployed_hosts, _ = EX5.deploy(EX5.Deployment(hosts=hosts,
                                          env_name="wheezy-x64-prod"))
        # Copy the jars required by Active Data
        EX.Put(hosts, [XP_BASEDIR + '/jars']).run()

        # Active Data server
        AD_node = filter(lambda x: self.options.ad_cluster in x, deployed_hosts)[0]
        EX.Put([AD_node], [XP_BASEDIR + '/server.policy']).run()

        # Hadoop Cluster
        deployed_hosts.remove(AD_node)
        workers = [EX.Host(host) for host in list(deployed_hosts)]
        EX.Put(workers, ['~/.ssh/']).run()

        logger.info('Creating Hadoop cluster on %s',
                    ' '.join([style.host(host.address) for host in workers]))
        cluster = HadoopCluster(workers)
        cluster.bootstrap('hadoop-1.2.1.tar.gz')
        cluster.initialize()
        cluster.start()

        return AD_node, cluster

    def get_hosts(self):
        """Returns the hosts from an existing reservation if provided, or from
        a new reservation"""
        logger.info('Retrieving hosts list')
        self.site = get_cluster_site(self.options.ad_cluster)
        self.job_id = self.options.job_id if self.options.job_id\
            else self._make_reservation(self.site)
        EX5.wait_oar_job_start(self.job_id, self.site)
        return EX5.get_oar_job_nodes(self.job_id, self.site)

    def _make_reservation(self, site):
        """Make a new reservation"""
        elements = {self.options.ad_cluster: 1,
                    self.options.work_cluster: self.options.n_nodes}
        logger.info('Finding slot for the experiment '
                    '\nActiveData %s:1\nHadoop %s:%s',
                    style.host(self.options.ad_cluster).rjust(5),
                    style.emph(self.options.work_cluster).rjust(5),
                    style.emph(self.options.n_nodes))
        planning = funk.get_planning(elements)
        slots = funk.compute_slots(planning, walltime=self.options.walltime, excluded_elements=EXCLUDED_ELEMENTS)
        slot = funk.find_free_slot(slots, elements)
        startdate = slot[0]
        resources = funk.distribute_hosts(slot[2], elements, excluded_elements=EXCLUDED_ELEMENTS)
        jobs_specs = funk.get_jobs_specs(resources, name=job_name, excluded_elements=EXCLUDED_ELEMENTS)
        print jobs_specs
        sub, site = jobs_specs[0]
        sub.additional_options = "-t deploy"
        sub.reservation_date = startdate
        jobs = EX5.oarsub([(sub, site)])
        job_id = jobs[0][0]
        logger.info('Job %s will start at %s', style.emph(job_id),
                style.log_header(EX.time_utils.format_date(startdate)))
        return job_id

        def _get_logs(self, AD_node, hadoop_cluster):
            # Output from the jobtracker
            EX.Get([hadoop_cluster.master], HADOOP_LOG_PATH +
                   "hadoop-root-jobtracker-{{{host}}}.log",
                   local_location=self.result_dir).run()
            
            # Output from the tasktrackers
            EX.Get([hadoop_cluster.hosts], HADOOP_LOG_PATH +
                   "hadoop-root-tasktracker-{{{host}}}.log",
                   local_location=self.result_dir).run()

def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def timestamp2str(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def prediction(timestamp):
    start = timestamp2str(timestamp)
    ad_hadoop._log("Waiting for job to start (prediction: {0})".format(start), False)


class FileOutputHandler(ProcessOutputHandler):
    __file = None

    def __init__(self, path):
        super(ProcessOutputHandler, self).__init__()
        self.__file = open(path, 'a')

    def __del__(self):
        self.__file.flush()
        self.__file.close()

    def read(self, process, string, eof=False, error=False):
        self.__file.write(string)
        self.__file.flush()

    def read_line(self, process, string, eof=False, error=False):
        self.__file.write(time.localtime().strftime("[%d-%m-%y %H:%M:%S"))
        self.__file.write(' ')
        self.__file.write(string)
        self.__file.flush()


###################
# Main
###################
if __name__ == "__main__":
    engine = ad_hadoop()
    engine.start()
   