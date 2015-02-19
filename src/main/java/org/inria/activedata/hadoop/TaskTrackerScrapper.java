package org.inria.activedata.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.inria.activedata.runtime.client.ActiveDataClient;
import org.inria.activedata.runtime.client.ActiveDataClientDriver;
import org.inria.activedata.runtime.communication.rmi.RMIDriver;

/**
 * This program reads logs from a Hadoop TaskTracker and publishes the
 * Active Data life cycle transitions it detects from the logs.
 *
 */
public class TaskTrackerScrapper extends Thread {
	public static final String SUPPORTED_HADOOP_VERSION = "1.2.1";
	private static final int AD_DEFAULT_PORT = 1200;

	private String adHost;
	private int adPort;
	private File logFile;

	private ActiveDataClient adClient;

	public TaskTrackerScrapper(String adHost, int adPort, String logPath) {
		this.adHost = adHost;
		this.adPort = adPort;
		this.logFile = new File(logPath);
	}

	@Override
	public void run() {
		// Wait for the file to exist
		System.out.print("Waiting for log file");
		while(!logFile.exists()) {
			try {
				// Sleep
				Thread.sleep(300);
			} catch (InterruptedException e) { 
				System.out.println(" Interrupted");
				return;
			}
		}
		System.out.println(String.format("\rLog file %s exists", logFile.getAbsolutePath()));

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
		System.out.println("Starting to read");
		while(!interrupted()) {
			String line = null;
			try {
				while((line = reader.readLine()) != null) {
					// TODO Do something with the string
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
	}

	/**
	 * Connect the Active Data Client to the Active Data Service using RMI.
	 * The address and port to the service are given to the object constructor.
	 */
	private void connectAdClient() {
		System.out.print("Connecting to the Active Data Service...");
		try {
			ActiveDataClientDriver driver = new RMIDriver(adHost, adPort);
			driver.connect();
			ActiveDataClient.init(driver);
			adClient = ActiveDataClient.getInstance();
		} catch(Exception e) {
			System.out.println(" Error");
			System.err.println(e);
			System.exit(44);
		}
		System.out.println(" Done");
	}

	private static void usage(String error, Options opt) {
		String syntax = String.format("java %s [-hv] [-p <port>] <adhost> <logfile>", TaskTrackerScrapper.class.getName());
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
		String version = TaskTrackerScrapper.class.getPackage().getImplementationVersion();
		System.out.println(String.format("Hadoop TaskTrackerTracker %s for Hadoop %s.", version, SUPPORTED_HADOOP_VERSION));
		System.exit(0);
	}

	@SuppressWarnings("static-access")
	public static void main(String[] args ) {
		System.out.println(System.getProperty("user.dir"));
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

		String adHost = null;
		int adPort = AD_DEFAULT_PORT;
		String logPath = null;

		CommandLineParser parser = new GnuParser();
		try {
			CommandLine cmd = parser.parse(options, args);

			if(cmd.hasOption('h'))
				usage(null, options);

			if(cmd.hasOption('v'))
				printVersion();

			if(cmd.hasOption('p'))
				adPort = Integer.parseInt(cmd.getOptionValue('p'));

			String[] leftovers = cmd.getArgs();
			if(leftovers.length != 2)
				usage("Invalid arguments.", options);

			adHost = leftovers[0];
			logPath = leftovers[1];

			if(adHost == null || logPath == null)
				usage("adhost and logfile are required.", options);
			System.out.println("adHost: " + adHost + "\nlogPath: " + logPath);
		} catch (ParseException e) {
			usage(e.getMessage(), options);
		}

		// Start the scrapper
		final TaskTrackerScrapper scrapper = new TaskTrackerScrapper(adHost, adPort, logPath);
		scrapper.start();

		// Handle interruptions
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				if(scrapper.isAlive()) {
					System.out.print("Stopping the scrapper...");
					try {
						scrapper.interrupt();
						scrapper.join();
					} catch (InterruptedException e) {
						System.out.println(" Error");
						e.printStackTrace();
					}
					System.out.println(" Done");
				}
			}
		});
	}
}