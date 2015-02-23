#!/usr/bin/env python

import traceback
import logging, time, datetime, signal
import pprint, os, sys, math
pp = pprint.PrettyPrinter(indent=4).pprint

import execo as EX
from execo.process import ProcessOutputHandler
import execo_g5k as EX5
from execo_g5k.api_utils import get_cluster_site
from execo_engine import Engine, ParamSweeper, logger, sweep, sweep_stats, slugify
EX.logger.setLevel(logging.ERROR)
logger.setLevel(logging.ERROR)

# Shortcut
funk = EX5.planning

NORMAL			= "\x1B[0m"
GREEN			= "\x1B[32m"
BOLD_MAGENTA	= "\x1B[35;1m"
RED				= "\x1B[31m"

OUT_FILE_FORMAT = 'events_per_sec_{0}W_{1}T'

JAR_NAME = 'active-data-lib-0.2.0.jar'

WALLTIME = 45 * 60

# Setup signal handler
def sighandler(signal, frame):
	if ad_hadoop._job is not None:
		ad_hadoop._log("\nInterrupted: killing oar job " + BOLD_MAGENTA + \
							str(ad_hadoop._job[0][0]) + NORMAL + "\n")
		EX5.oar.oardel(ad_hadoop._job)
	else:
		ad_hadoop._log("\nInterrupted: no job to kill")
	sys.exit(0)

signal.signal(signal.SIGINT, sighandler)
signal.signal(signal.SIGQUIT, sighandler)
signal.signal(signal.SIGTERM, sighandler)

class ad_hadoop(Engine):
	_stat = ""
	_job = None
	
	def __init__(self):
		super(ad_hadoop, self).__init__()
	
	def run(self):
		cluster = 'parapide'
		server_out_path = os.path.join(self.result_dir, "server.out")
		listener_out_path = os.path.join(self.result_dir, "listener.out")
		
		# Find a slot available for the experiment
		n_nodes = int(math.ceil(float(comb['n_clients']) / EX5.get_host_attributes(cluster + '-1')['architecture']['smt_size'])) + 1

		planning = funk.get_planning([cluster], out_of_chart = (n_nodes > 20))
			
		slots = funk.compute_slots(planning, WALLTIME)
		if slots is None:
			self._log("Could not find a slot for {0} nodes on {1}".format(n_nodes, cluster))
			sweeper.skip(comb)
			continue
		
		resources_wanted = {}
		resources_wanted[cluster] = n_nodes
		
		startdate, enddate, resources = funk.find_first_slot(slots, resources_wanted)
		
		self._log("Found a slot from {0} to {1}".format(timestamp2str(startdate), timestamp2str(enddate)))
		
		resources = funk.distribute_hosts(resources, resources_wanted)
		job_specs = funk.get_jobs_specs(resources)
		job_specs[0][0].job_type = 'allow_classic_ssh'
		job_specs[0][0].additional_options = '-t deploy'
			
		# Performing the submission on G5K
		site = get_cluster_site(cluster)
		self._log("Output will go to " + self.result_dir)
		
		self._log("Reserving {0} nodes on {1}".format(n_nodes, site))
		
		job = EX5.oarsub(job_specs)
		
		self.__class__._job = job
		
		# Sometimes oarsub fails silently
		if job[0][0] is None:
			print("\nError: no job was created")
			sweeper.skip(comb)
			continue
			
		# Wait for the job to start
		self._log("Waiting for job {0} to start...\n".format(BOLD_MAGENTA + str(job[0][0]) + NORMAL))
		EX5.wait_oar_job_start(job[0][0], job[0][1], prediction_callback = prediction)
		nodes = EX5.get_oar_job_nodes(job[0][0], job[0][1])
		
		# Deploying nodes
		deployment = EX5.Deployment(hosts = nodes, env_name='wheezy-x64-prod')
		run_deploy = EX5.deploy(deployment)
		nodes_deployed = run_deploy.hosts[0]
		
		# Copying active_data program on all deployed hosts
		EX.Put(nodes, 'active-data-lib-0.1.2.jar', connexion_params = {'user': 'root'}).run()
		EX.Put(nodes, 'server.policy', connexion_params = {'user': 'root'}).run()
			
		# Split the nodes
		ADservice = nodes[0] 
		clients = nodes[1:]
		
		self._log("Running experiment with {0} Hadoop Workers and {1}".format(len(clients), sizeof_fmt(input_size)))
		
		# Launching Server on one node
		out_handler = FileOutputHandler(server_out_path)
		cmd = "java -Djava.security.policy=server.policy -jar {1}".format(JAR_NAME)
		self._log("Running command " + cmd)
		launch_server = EX.Remote(cmd , [ADservice])
		launch_server.processes[0].stdout_handlers.append(out_handler)
		launch_server.processes[0].stderr_handlers.append(out_handler)
		launch_server.start()
		self._log("Active Data Service started on " + ADservice.address)
		time.sleep(2)
		
		if not launch_server.processes[0].running:
			self._log("Active Data Service crashed\n")
			process = launch_server.processes[0]
			if process.stdout is not None: print(GREEN + process.stdout + NORMAL)
			if process.stderr is not None: print(RED + process.stderr + NORMAL)
		
		# Creating the Hadoop cluster
		
		
		
		# Launching a scrapper per Hadoop TaskTracker, plus a scrapper for the JobTracker
		rank=0
		n_cores = EX5.get_host_attributes(clients[0])['architecture']['smt_size'];
		cores = nodes * n_cores
		cores = cores[0:comb['n_clients']] # Cut out the additional cores
		
		self._log("Launching {0} clients...".format(len(cores)))
		
		# Start the clients
		client_cmd = "java -Djava.security.policy=" + policy_path + " -cp " + jar_path + " org.inria.activedata.examples.perf.TransitionsPerSecond " + \
						"{0} {1} {2} {3} {4}".format(server.address, 1200, "{{range(len(cores))}}", len(cores), comb['n_transitions'])
		client_out_handler = FileOutputHandler(os.path.join(self.result_dir, "clients.out"))
		client_request = EX.TaktukRemote(client_cmd, cores)
			
		for process in client_request.processes:
			process.stdout_handlers.append(client_out_handler)
			process.stderr_handlers.append(client_out_handler)
		
		client_request.run()
		
		if not client_request.ok:
			# Some client failed, please panic
			self._log("One or more client process failed. Enjoy reading their outputs.")
			self._log("OUTPUT STARTS -------------------------------------------------\n")
			for process in client_request.processes:
				print("----- {0} exited with code: {1}".format(process.host.address, process.exit_code))
				if process.stdout is not None: print(GREEN + process.stdout + NORMAL)
				if process.stderr is not None: print(RED + process.stderr + NORMAL)
			self._log("OUTPUT ENDS ---------------------------------------------------\n")
			sweeper.skip(comb)
			launch_server.kill()
			launch_server.wait()
		else:
			# Waiting for server to end
			if not launch_server.processes[0].running and not launch_server.ok:
				self._log("Server crashed\n")
				process = launch_server.processes[0]
				if process.stdout is not None: print(GREEN + process.stdout + NORMAL)
				if process.stderr is not None: print(RED + process.stderr + NORMAL)
			launch_server.wait()
			
			# Getting log files
			#distant_path = OUT_FILE_FORMAT.format(len(cores), comb['n_transitions'])
			#local_path = distant_path
			
			#EX.Get([server], distant_path).run()
			
			#EX.Local('mv ' + distant_path + ' ' + os.path.join(self.result_dir, local_path)).run()
			
			#EX.Get([server], 'client_*.out', local_location = self.result_dir)
			#EX.Remote('rm -f client_*.out', [server])
			
			self._log("Finishing experiment with {0} clients and {1} transitions per client".format(comb['n_clients'], comb['n_transitions']))
			
			sweeper.done(comb)
			
		sub_comb = sweeper.get_next (filtr = lambda r: filter(lambda s: s["n_clients"] == comb['n_clients'], r))
		self._updateStat(sweeper.stats())
				
def sizeof_fmt(num, suffix='B'):
	for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
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
    try:
    	engine.start()
    except Exception as e:
    	engine._log(traceback.format_exc())

    	job = engine.__class__._job
    	if job is not None:
    		EX5.oar.oardel(job)
    		engine._log("Deleted job " + BOLD_MAGENTA + str(job[0][0]) + NORMAL + "\n")