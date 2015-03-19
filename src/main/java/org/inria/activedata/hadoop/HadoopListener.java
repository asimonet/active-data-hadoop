package org.inria.activedata.hadoop;

import org.inria.activedata.model.LifeCycle;
import org.inria.activedata.runtime.client.ActiveDataClient;
import org.inria.activedata.runtime.client.ActiveDataClientDriver;
import org.inria.activedata.runtime.communication.rmi.RMIDriver;

/**
 * Watches the life cycle of a file in HDFS and its associated
 * Hadoop jobs.
 * 
 * @author Anthony SIMONET <anthony.simonet@inria.fr>
 */
public class HadoopListener {
	/**
	 * Defines the order in which the places will be printed
	 */
	private static String[] places = {
		"Job submitted",
		"Job started",
		"Job done",
		"Map submitted",
		"Map received",
		"Map started",
		"Map output sent",
		"Reduce submitted",
		"Reduce received",
		"Reduce started",
	};

	/**
	 * Loop through the places and print their name and the
	 * number of tokens they contain
	 */
	private static void printState(LifeCycle lc) {
		for(String place: places) {
			System.out.println(String.format("%s\t\t%d", place, lc.getTokens("Hadoop." + place).size()));
		}
		System.out.println("----------------------------------------");
	}

	private static void usage() {
		System.out.println("Usage: java org.inria.activedata.hadoop.HadoopListener <ad host> <ad port> <HDFS path>");
		System.exit(0);
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 3)
			usage();

		// Command line arguments
		String adHost = args[0];
		int adPort = Integer.parseInt(args[1]);
		String filePath = args[2];

		// Connect to the Active Data Service
		ActiveDataClientDriver driver = new RMIDriver(adHost, adPort);
		ActiveDataClient.init(driver);
		driver.connect();
		final ActiveDataClient ad = ActiveDataClient.getInstance();
		
		LifeCycle lc = null;

		// Now loop
		while(true) {
			lc = ad.getLifeCycle("HDFS", filePath);
			
			if(lc != null)
				printState(lc);
			try {
				Thread.sleep(1000);
			} catch(InterruptedException e) {
				// Ignore
			}
		}
	}
}
