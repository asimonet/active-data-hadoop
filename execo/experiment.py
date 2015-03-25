#!/usr/bin/env python

import traceback
import logging, time, datetime, signal
import pprint, os, sys, math
pp = pprint.PrettyPrinter(indent=4).pprint
from threading import Thread
from time import sleep

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

default_ad_cluster      = 'parapide'
default_work_cluster    = 'paranoia'
default_n_nodes         = 5
default_walltime        = "3:00:00"

XP_BASEDIR = '/home/ansimonet/active_data_hadoop'
AD_JAR_NAME = 'active-data-lib-0.2.0.jar'
HADOOP_LOG_PATH = '/tmp/hadoop/logs/'
AD_RESULT_PATH = '/tmp/transitions_per_second.log'

HADOOP_OPTIONS = '-Ddfs.namenode.handler.count=40 ' + \
    '-Ddfs.datanode.handler.count=10' + \
    '-Dmapreduce.map.output.compress=true ' + \
    '-Dmapreduce.map.output.compress.codec=org.apache.hadoop.io.compress.GzipCodec ' + \
    '-Dmapred.child.java.opts=\'-Xms%d\''
    
 
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
                                       type=int,
                                       help="The number of nodes to use.")
        self.options_parser.add_option("--n-reducers",
                                       type=int,
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
            
            if hosts is None:
                logger.error("Cannot get host for request")
                exit(1)

            # Deploying OS and copying required file
            AD_node, hadoop_cluster = self.setup_hosts(hosts)
            
            # Starting the processes
            server = self._start_active_data_server(AD_node)
            listener = self._start_active_data_listener(AD_node)
            
            # Terasort takes the number of 100-byte rows to generate
            data_size_bytes = self.options.size * 1000 * 1000 * 1000
            data_size = data_size_bytes / 100
            
            # Use a mapper per core
            attr = EX5.get_host_attributes(self.options.work_cluster + '-1')
            n_cpu_per_node = attr['architecture']['smt_size']
            mem_per_node = attr['main_memory']['ram_size']

            n_mappers = int(n_cpu_per_node) * self.options.n_nodes
            mem_per_task = (int(mem_per_node) - 1000) / n_cpu_per_node # leave 1Gb out for the system
            logger.info("The experiment will use %s mappers and %s reducers",
                        style.emph(n_mappers),
                        style.emph(self.options.n_reducers))
            logger.info("Using %s of memory for each task", style.emph(sizeof_fmt(mem_per_task)))
            
            # First Hadoop job to generate the dataset
            hadoop_stdout_path = os.path.join(self.result_dir, "hadoop.stdout")
            hadoop_stderr_path = os.path.join(self.result_dir, "hadoop.stderr")
            fout = open(hadoop_stdout_path, 'w')
            ferr = open(hadoop_stderr_path, 'w')
            
            logger.info("Generating a %s input file" % (sizeof_fmt(data_size_bytes)))
            generate = HadoopJarJob('jars/hadoop-examples-1.2.1.jar',
                               "teragen -Dmapred.map.tasks=%d -Dmapred.tasktracker.map.tasks.maximum=%s %s %s %sinput" % (n_mappers, n_cpu_per_node, HADOOP_OPTIONS % mem_per_node, data_size, job_path))
            gen_out, gen_err = hadoop_cluster.execute_job(generate)
            fout.write(gen_out)
            ferr.write(gen_err)
            
            # Create an Active Data life cycle for the input dataset
            cmd = "java -cp \"jars/*\" org.inria.activedata.examples.cmdline.PublishNewLifeCycle %s %s %s" % \
                (AD_node, "org.inria.activedata.hadoop.model.HDFSModel", job_path + "input")
            create_lc = EX.Process(cmd)
            create_lc.start()
            create_lc.wait()
            
            logger.info(create_lc.stdout)
            if not create_lc.ok:
                logger.error(create_lc.stderr)
                exit(1)
            
            # Start the second Hadoop job on a separate thread: the actual benchmark
            sort = HadoopJarJob('jars/hadoop-examples-1.2.1.jar',
                               "terasort -Dmapred.reduce.tasks=%d -Dmapred.map.tasks=%d -Dmapred.tasktracker.map.tasks.maximum=%s -Dmapred.tasktracker.reduce.tasks.maximum=%s %s %sinput "
                               "%soutput" % (self.options.n_reducers, n_mappers, n_cpu_per_node, n_cpu_per_node, HADOOP_OPTIONS % mem_per_node, job_path,
                                             job_path))
            
            def run_sort(sort, fout, ferr):
                sort_out, sort_err = hadoop_cluster.execute_job(sort)
                fout.write(sort_out)
                ferr.write(sort_err)
                
            sort_thread = Thread(target=run_sort, args=(sort, fout, ferr), name="sort")
            sort_thread.start()

            # Publish the composition transition as soon as we got the Hadoop job_id
            while sort.job_id == "unknown":
                sleep(0.1)
            
            logger.info("The id of the sort job is " + sort.job_id)
            cmd = "java -cp \"jars/*\" org.inria.activedata.examples.cmdline.PublishTransition %s -m \
                org.inria.activedata.hadoop.model.HDFSModel -t 'HDFS.create Hadoop job' -sid HDFS \
                -uid %sinput -newId %s" \
                % (AD_node, job_path, sort.job_id[4:])
            compose = EX.Process(cmd)
            compose.start()
            compose.wait()
            
            logger.info(compose.stdout)
            if(not compose.ok):
                logger.error(compose.stderr)
                exit(1)
                
            # Start the scrappers, tell them to pay attention only to the sort job
            clients = self._start_active_data_clients(AD_node, hadoop_cluster, only=sort.job_id[4:])
                
            # Wait for the sort job to complete
            sort_thread.join()
            
            # Leave a few seconds for all the scrapers to do their job
            sleep(600)
            
            fout.close()
            ferr.close()
            
            # Terminate everything
            clients.kill()
            listener.kill()
            
            # Kindly ask the service to quit
            EX.Process('java -cp "jars/*" org.inria.activedata.examples.cmdline.EndExperiment ' \
                    + server.host.address\
                    + ' ' + AD_RESULT_PATH).run()
            
            # Get the logs back from the machines
            self._get_logs(hadoop_cluster, server.host)

        finally:
            hadoop_cluster.stop()
            if not self.options.keep_alive:
                EX5.oardel([(self.job_id, self.site)])
                
        exit()
    
    def _start_active_data_clients(self, server, hadoop_cluster, port=1200,
                                   hdf_path='/root/hduser/terasort-input', only=""):
        """Return a started client action"""
        
        if only != "":
            only = "-o " + only

        cmd = "java -cp 'jars/*' -Dlog4j.configurationFile=log4j2.xml " + \
            "org.inria.activedata.hadoop.HadoopScrapper " +\
            server + " " + HADOOP_LOG_PATH + "hadoop-root-tasktracker-{{{host}}}.log " + only
        tasktracker = EX.Remote(cmd, hadoop_cluster.hosts)
        
        # Don't print a warning when the jobs are killed
        for p in tasktracker.processes:
            p.nolog_exit_code = p.ignore_exit_code = True
        cmd = "java -cp 'jars/*' -Dlog4j.configurationFile=log4j2.xml " + \
            "org.inria.activedata.hadoop.HadoopScrapper " +\
            server + " " + HADOOP_LOG_PATH + "hadoop-root-jobtracker-{{{host}}}.log " + only
        jobtracker = EX.Remote(cmd, [hadoop_cluster.master])
        for p in jobtracker.processes:
            p.nolog_exit_code = p.ignore_exit_code = True

        clients = EX.ParallelActions([tasktracker, jobtracker])
        
        # Setup output handlers
        for p in clients.processes:
            # Treat the jobtracker scraper differently
            if "jobtracker" in p.remote_cmd:
                stdout = os.path.join(self.result_dir, 'scrapper-jobtracker.stdout')
                stderr = os.path.join(self.result_dir, 'scrapper-jobtracker.stderr')
            else:
                stdout = os.path.join(self.result_dir, 'scrapper-' + p.host.address + '.stdout')
                stderr = os.path.join(self.result_dir, 'scrapper-' + p.host.address + '.stderr')

            p.stdout_handlers.append(stdout)
            p.stderr_handlers.append(stderr)
        
        clients.start()

        return clients

    def _start_active_data_listener(self, ad_server, port=1200,
                                   hdf_path='/root/hduser/terasort-input'):
        """Return a started listener process"""
        cmd = "java -cp 'jars/*' org.inria.activedata.hadoop.HadoopListener %s %s %s" \
                % (ad_server, port, hdf_path)
        out_path = os.path.join(self.result_dir, "listener")
        listener = EX.SshProcess(cmd, ad_server)
        listener.stdout_handlers.append(out_path + ".stdout")
        listener.stderr_handlers.append(out_path + ".stderr")
        listener.nolog_exit_code = listener.ignore_exit_code = True
        listener.start()
        return listener

    def _start_active_data_server(self, ad_server):
        """Return a started server process"""
        stdout_path = os.path.join(self.result_dir, "server.stdout")
        stderr_path = os.path.join(self.result_dir, "server.stderr")
        options = "-Djava.security.policy=server.policy " \
            + "-server -Xmx1G -Xmx10G"
        cmd = "java " + options + " -cp \"jars/*\" org.inria.activedata.examples.cmdline.RunService -vv"
        logger.info("Running command " + cmd)
        server = EX.SshProcess(cmd, ad_server)
        server.stdout_handlers.append(stdout_path)
        server.stderr_handlers.append(stderr_path)
        server.nolog_exit_code = server.ignore_exit_code = True
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
        EX.Put(hosts, [XP_BASEDIR + '/server.policy']).run()
        EX.Put(hosts, [XP_BASEDIR + '/log4j2.xml']).run()

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
            
        if not self.job_id:
            return None
        
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
        
        if not slot[0]:
            return None

        startdate = slot[0]
        resources = funk.distribute_hosts(slot[2], elements, excluded_elements=EXCLUDED_ELEMENTS)
        jobs_specs = funk.get_jobs_specs(resources, name=job_name, excluded_elements=EXCLUDED_ELEMENTS)
        print jobs_specs
        sub, site = jobs_specs[0]
        sub.additional_options = "-t deploy"
        sub.reservation_date = startdate
        sub.walltime = self.options.walltime
        jobs = EX5.oarsub([(sub, site)])
        job_id = jobs[0][0]
        logger.info('Job %s will start at %s', style.emph(job_id),
                style.log_header(EX.time_utils.format_date(startdate)))
        return job_id

    def _get_logs(self, hadoop_cluster, service_host):
        # Output from the jobtracker
        EX.Get([hadoop_cluster.master], [HADOOP_LOG_PATH +
               "hadoop-root-jobtracker-{{{host}}}.log"],
               local_location=self.result_dir).run()

        # Output from the tasktrackers
        logger.info(hadoop_cluster.hosts)
        EX.Get(hadoop_cluster.hosts, [HADOOP_LOG_PATH +
               "hadoop-root-tasktracker-{{{host}}}.log"],
               local_location=self.result_dir).run()
        
        # The actual measure, on the service
        EX.Get([service_host], [AD_RESULT_PATH], local_location=self.result_dir).run()
        EX.Remote('rm -f ' + AD_RESULT_PATH, [service_host]).run() # We don't want to interfere with the next experiment

def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z']:
        if abs(num) < 1000.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1000.0
    return "%.1f%s%s" % (num, 'Y', suffix)


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
   