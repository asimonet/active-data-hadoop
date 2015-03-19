package org.inria.activedata.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.inria.activedata.hadoop.model.HDFSModel;
import org.inria.activedata.model.InvalidTransitionException;
import org.inria.activedata.model.LifeCycle;
import org.inria.activedata.model.Transition;
import org.inria.activedata.model.TransitionNotEnabledException;
import org.inria.activedata.runtime.client.ActiveDataClient;
import org.inria.activedata.runtime.client.ActiveDataClientDriver;
import org.inria.activedata.runtime.communication.rmi.RMIDriver;

/**
 * This program reads logs from a Hadoop TaskTracker and publishes the
 * Active Data life cycle transitions it detects from the logs.
 *
 * @author Anthony SIMONET <anthony.simonet@inria.fr>
 */
public class HadoopScrapper extends Thread {
	public static final String SUPPORTED_HADOOP_VERSION = "1.2.1";
	private static final int AD_DEFAULT_PORT = 1200;
	private static final int QUEUE_INITIAL_CAPACITY = 5000;

	private static Logger log = LogManager.getLogger(HadoopScrapper.class);

	private String adHost;
	private int adPort;
	private File logFile;
	private static boolean debug = false;
	
	// We store the transitions we need here to avoid unnecessary lookups
	private ActiveDataClient adClient;
	private Transition submitJob;
	private Transition startJob;
	private Transition endJob;
	private Transition submitMap;
	private Transition assignMap;
	private Transition startMap;
	private Transition endMap;
	private Transition submitReduce;
	private Transition assignReduce;
	private Transition startReduce;
	private Transition endReduce;
	private Transition shuffle;

	/**
	 * Any jobId that is not in this set will be ignored
	 */
	private Set<String> only;

	/**
	 * Transitions to be published are enqueued here.
	 * The priority queue publishes 'upstream' transitions first for more
	 * efficiency.
	 */
	private BlockingQueue<Publication> publicationQueue;

	/**
	 * The consumer thread.
	 */
	private Thread publishingThread;

	public HadoopScrapper(String adHost, int adPort, String logPath, String[] only) {
		this.adHost = adHost;
		this.adPort = adPort;
		this.logFile = new File(logPath);

		// Initialize the transitions
		HDFSModel model = new HDFSModel();
		submitJob = (Transition) model.getTransition("Hadoop.Submit job");
		startJob = (Transition) model.getTransition("Hadoop.Start job");
		endJob = (Transition) model.getTransition("Hadoop.End job");
		submitMap = (Transition) model.getTransition("Hadoop.Submit map");
		assignMap = (Transition) model.getTransition("Hadoop.Assign map");
		startMap = (Transition) model.getTransition("Hadoop.Start map");
		endMap = (Transition) model.getTransition("Hadoop.End map");
		submitReduce = (Transition) model.getTransition("Hadoop.Submit reduce");
		assignReduce = (Transition) model.getTransition("Hadoop.Assign reduce");
		startReduce = (Transition) model.getTransition("Hadoop.Start reduce");
		endReduce = (Transition) model.getTransition("Hadoop.End reduce");
		shuffle = (Transition) model.getTransition("Hadoop.Shuffle");

		if(only != null)
			this.only = new HashSet<String>(Arrays.asList(only));
		
		final HashMap<Transition, Integer> priorities = new HashMap<Transition, Integer>(); 
		priorities.put(submitJob, 0);
		priorities.put(startJob, 1);
		priorities.put(submitMap, 2);
		priorities.put(submitReduce, 2);
		priorities.put(assignMap, 3);
		priorities.put(assignReduce, 3);
		priorities.put(startMap, 4);
		priorities.put(shuffle, 5);
		priorities.put(startReduce, 6);
		priorities.put(endMap, 7);
		priorities.put(endReduce, 7);
		priorities.put(endJob, 8);

		publicationQueue = new PriorityBlockingQueue<Publication>(QUEUE_INITIAL_CAPACITY, new Comparator<Publication>() {
			@Override
			public int compare(Publication p1, Publication p2) {
				return priorities.get(p1.t) - priorities.get(p2.t);
			}
		});

		publishingThread = new Thread() {
			public void run() {
				log.info("Publishing thread started");

				// We stop when we have been interrupted, but only after the queue has been emptied
				while(!interrupted()) {
					Publication p = null;

					try {
						p = publicationQueue.take();
					} catch (InterruptedException e) {
						// Ignore
					}
					
					if(p == null)
						continue;

					// Do the thing, put the publication back on the queue if it failed
					if(!publishTransitionForJob(p)) {
						try {
							publicationQueue.put(p);
							log.warn("Failed to publish " + p.t.getName() + " for " + p.taskSubId);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					else
						log.debug(String.format("[%d] " + p, publicationQueue.size()));
				}
				
				log.info("Publishing thread interrupted");
			}
		};

		publishingThread.start();
	}

	@Override
	public void run() {
		// Wait for the file to exist
		log.info("Waiting for log file");
		while(!logFile.exists()) {
			try {
				// Sleep
				Thread.sleep(300);
			} catch (InterruptedException e) { 
				log.warn(" Interrupted");
				return;
			}
		}
		log.info(String.format("Log file %s exists", logFile.getAbsolutePath()));

		// Check that the file is readable
		if(!logFile.canRead()) {
			System.err.println("Error: cannot read file " + logFile.getAbsolutePath());
			System.exit(42);
		}
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(logFile));
		} catch (FileNotFoundException e) {
			System.err.println("Error: could not read file " + logFile.getAbsoluteFile());
			return;
		}

		// Connect to the Active Data Service
		connectAdClient();

		// Start the loop
		log.info("Starting to read");
		HadoopLogParser parser = new HadoopLogParser();
		HadoopLogParser.LogEntry entry = parser.new LogEntry();

		while(!interrupted()) {
			String line = null;
			try {
				while((line = reader.readLine()) != null) {
					entry = parser.parse(line, entry);

					switch(entry.entryType) {
					case JOB_SUBMITTED:
						enqueuePublication(submitJob, entry.jobId, null, String.format("Job %s submitted", entry.jobId));
						break;
					case JOB_STARTED:
						enqueuePublication(startJob, entry.jobId, null, String.format("Job %s started", entry.jobId));
						break;
					case JOB_DONE:
						enqueuePublication(endJob, entry.jobId, null, String.format("Job %s done", entry.jobId));
						break;
					case MAP_SUBMITTED:
						enqueuePublication(submitMap, entry.jobId, entry.taskId, String.format("Map task %s for job %s submitted", entry.taskSubId, entry.jobId));
						break;
					case MAP_RECEIVED:
						enqueuePublication(assignMap, entry.jobId, entry.taskId, String.format("Received new map task %s for job %s", entry.taskSubId, entry.jobId));
						break;
					case MAP_STARTED:
						enqueuePublication(startMap, entry.jobId, entry.taskId, String.format("Map task %s for job %s started", entry.taskSubId, entry.jobId));
						break;
					case MAP_OUTPUT_SENT:
						enqueuePublication(shuffle, entry.jobId, entry.taskId, String.format("Sent map output from task %s for job %s", entry.taskSubId, entry.jobId));
						break;
					case MAP_DONE:
						enqueuePublication(endMap, entry.jobId, entry.taskId, String.format("Map task %s for job %s done", entry.taskSubId, entry.jobId));
						break;
					case REDUCE_SUBMITTED:
						enqueuePublication(submitReduce, entry.jobId, entry.taskId, String.format("Reduce task %s for job %s submitted", entry.taskSubId, entry.jobId));
						break;
					case REDUCE_RECEIVED:
						enqueuePublication(assignReduce, entry.jobId, entry.taskId, String.format("Received new reduce task %s for job %s", entry.taskSubId, entry.jobId));
						break;
					case REDUCE_STARTED:
						enqueuePublication(startReduce, entry.jobId, entry.taskId, String.format("Reduce task %s for job %s started", entry.taskSubId, entry.jobId));
						break;
					case REDUCE_DONE:
						enqueuePublication(endReduce, entry.jobId, entry.taskId, String.format("Reduce task %s for job %s done", entry.taskSubId, entry.jobId));
						break;

					default:
					case NONE:
						break; // Just any other line
					}
				}
			} catch (IOException e) {
				System.err.println("Error: could not read file " + logFile.getAbsoluteFile());
				System.exit(43);
			}
		}

		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// We have been interrupted, wait for the publishing thread
		publishingThread.interrupt();
		try {
			publishingThread.join();
		} catch (InterruptedException e) {
			// Ignore
		}
	}

	private boolean publishTransitionForJob(Publication p) {
		if(debug)
			return true;
		
		log.info(String.format("Trying to publish %s %s for job %s", p.t.getName(), p.taskSubId, p.jobId));

		// Get the up to date life cycle otherwise the transition might appear as disabled when it's actually not
		LifeCycle lifeCycle = adClient.getLifeCycle("Hadoop", p.jobId);

		/* 
		 * The life cycle may not already exists, in which case we loop
		 */
		while(lifeCycle == null) {
			log.info("Life cycle is null for id " + p.jobId);
			lifeCycle = adClient.getLifeCycle("Hadoop", p.jobId);

			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// Ignore
			}
		}
		
		/* 
		 * If the transition is not published, it will be retried later.
		 * This is an additional (and not so pretty) security to avoid deadlocks.
		 */
		boolean published = false;
		try {
			adClient.publishTransition(p.t, lifeCycle);
			published = true;
		} catch (TransitionNotEnabledException tne) {
			System.err.println(tne.getMessage());
		} catch (InvalidTransitionException ite) {
			System.err.println(ite.getMessage());
		}
		
		return published;
	}

	/**
	 * Connect the Active Data Client to the Active Data Service using RMI.
	 * The address and port to the service are given to the object constructor.
	 */
	private void connectAdClient() {
		if(debug)
			return;

		log.info("Connecting to the Active Data Service...");
		try {
			ActiveDataClientDriver driver = new RMIDriver(adHost, adPort);
			driver.connect();
			ActiveDataClient.init(driver);
			adClient = ActiveDataClient.getInstance();
		} catch(Exception e) {
			log.warn(e.toString());
			System.exit(44);
		}
		log.info("Connected to the Active Data Service");
	}

	private void enqueuePublication(Transition trans, String jobId, String taskSubId, String message) {
		if(only != null && !only.contains(jobId))
			return;
		
		log.debug(String.format("[%d] Enqueueing %s %s for job %s", publicationQueue.size(), trans.getName(), taskSubId, jobId));
		
		// The queue is already synchronized
		boolean inserted = false;
		while(!inserted) {
			try {
				publicationQueue.put(new Publication(trans, jobId, taskSubId, message));
				inserted = true;
			} catch (InterruptedException e) {
				// Interrupted while waiting, interrupted == false
			}
		}
	}

	class Publication {
		Transition t;
		String jobId;
		String taskSubId;
		String message; // Just for debugging

		public Publication(Transition t, String jobId, String taskSubId, String message) {
			this.t = t;
			this.jobId = jobId;
			this.taskSubId = taskSubId;
			this.message = message;
		}

		public String toString() {
			return message;
		}
	}

	private static void usage(String error, Options opt) {
		String syntax = String.format("java %s [-hv] [-p <port>] <adhost> <logfile> [-o <job id>...]", HadoopScrapper.class.getName());
		if(error == null)
			error = "";
		String header = "This program reads the given Hadoop TaskTracker log file and publishes the "
				+ "Active Data life cycle transitions it detects to the given Active Data Service. If the "
				+ "given log file does not exist, the program will wait for it to be created.";

		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp(syntax, header, opt, error);
		System.exit(error.equals("")? 0:12);
	}

	/**
	 * Print the program version and exits.
	 */
	private static void printVersion() {
		String version = HadoopScrapper.class.getPackage().getImplementationVersion();
		System.out.println(String.format("Hadoop TaskTrackerTracker %s for Hadoop %s.", version, SUPPORTED_HADOOP_VERSION));
		System.exit(0);
	}

	@SuppressWarnings("static-access")
	public static void main(String[] args ) {
		// Command line arguments
		Options options = new Options();

		options.addOption("h", false, "Print this message.");
		options.addOption("v", false, "Print the version and exit.");

		options.addOption(OptionBuilder
				.withArgName("port")
				.hasArg()
				.withDescription(String.format("Port number the Active Data Service is"
						+ "listening on. (Default:%s)", AD_DEFAULT_PORT))
						.create('p'));

		options.addOption(OptionBuilder
				.withArgName("job ids")
				.hasArgs()
				.withDescription("Publish transitions only for the job ids provided")
				.create('o'));
		
		options.addOption(OptionBuilder
				.withArgName("Debug mode")
				.withDescription("Do not perform calls to the Active Data Service, only "
						+ "print debug messages.")
				.create('d'));

		String adHost = null;
		int adPort = AD_DEFAULT_PORT;
		String logPath = null;
		String[] only = null;

		CommandLineParser parser = new GnuParser();
		try {
			CommandLine cmd = parser.parse(options, args);

			if(cmd.hasOption('h'))
				usage(null, options);

			if(cmd.hasOption('v'))
				printVersion();

			if(cmd.hasOption('p'))
				adPort = Integer.parseInt(cmd.getOptionValue('p'));

			if(cmd.hasOption('o')) {
				only = cmd.getOptionValues('o');
				log.info("The following job ids only will be watched:");
				for(String s: only)
					log.info(s);
			}
			
			if(cmd.hasOption('d'))
				debug = true;

			String[] leftovers = cmd.getArgs();
			if(leftovers.length != 2)
				usage("Invalid arguments.", options);

			adHost = leftovers[0];
			logPath = leftovers[1];

			if(adHost == null || logPath == null)
				usage("adhost and logfile are required.", options);

			log.info("adHost: " + adHost + "\nlogPath: " + logPath);
			if(debug)
				log.warn("\nDebug mode enabled: no API call will be performed!\n");
		} catch (ParseException e) {
			usage(e.getMessage(), options);
		}

		// Start the scrapper
		final HadoopScrapper scrapper = new HadoopScrapper(adHost, adPort, logPath, only);
		scrapper.start();

		// Handle interruptions
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				if(scrapper.isAlive()) {
					log.info("Stopping the scrapper...");
					try {
						scrapper.interrupt();
						scrapper.join();
					} catch (InterruptedException e) {
						System.out.println(" Error");
						e.printStackTrace();
					}
					log.info(" Done");
				}
			}
		});
	}
}