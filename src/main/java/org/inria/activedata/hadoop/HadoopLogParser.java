package org.inria.activedata.hadoop;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parse a Hadoop TaskTracker or JobTracker log file and return events.
 * 
 * @author Anthony SIMONET <anthony.simonet@inria.fr>
 */
public class HadoopLogParser {
	private static final String IP_TEMPLATE = "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}";
	private static final String HOSTNAME_TEMPLATE = "[\\w\\-\\.]+";

	private static final String JOB_SUBMITTED_TEMPLATE = "Job job_(\\w+) added successfully for user '(\\w+)' to queue '(\\w+)'" ;
	private static final String JOB_STARTED_TEMPLATE = String.format("IP=%s	OPERATION=SUBMIT_JOB	TARGET=job_(\\w+)	RESULT=SUCCESS",
			IP_TEMPLATE);
	private static final String JOB_ENDED_TEMPLATE = "Job job_(\\w+) has completed successfully";
	private static final String STARTUP_IP_TEMPLATE = String.format("STARTUP_MSG:   host = (%s)/(%s)", HOSTNAME_TEMPLATE, IP_TEMPLATE);
	private static final String TASK_SUBMITTED_TEMPLATE = String.format("Adding task \\((MAP|REDUCE)\\) 'attempt_(\\w+)' to tip task_(\\w+), for tracker '([%s]):([%s])/(%s):55578'",
			HOSTNAME_TEMPLATE,
			HOSTNAME_TEMPLATE,
			IP_TEMPLATE);
	private static final String TASK_RECEIVED_TEMPLATE = "LaunchTaskAction (registerTask): (\\w+) task's state:UNASSIGNED";
	private static final String TASK_STARTED_TEMPLATE = "JVM with ID: (\\w+) given task: (\\w+)";
	//private static final String TASK_DONE_TEMPLATE = "Task (\\w+) is done.";
	private static final String TASK_DONE_TEMPLATE = "Removing task '(\\w+)'";
	private static final String TRANSFER_DONE_TEMPLATE = String.format("clienttrace: src: (%s):(\\d+), dest: (%s):(\\d+), bytes: (\\d+), op: MAPRED_SHUFFLE, cliID: (\\w+), duration: (\\d+)",
			IP_TEMPLATE,
			IP_TEMPLATE);
	
	private static final String TASK_ID_TEMPLATE = "attempt_(\\d{12})_(\\d{4})_(m|r)_(\\d{6})_(\\d+)";
	
	
	private static final String GET_JOB_STATUS_COMMAND = "/tmp/hadoop/bin/hadoop job -status job_(%s)";
	
	private String myIp;
	private String myHost;
	
	private Pattern startupIpPattern;
	private Pattern jobSubmittedPattern;
	private Pattern jobStartedPattern;
	private Pattern jobEndedPattern;
	private Pattern taskSubmittedPattern;
	private Pattern taskReceivedPattern;
	private Pattern taskStartedPattern;
	private Pattern taskDonePattern;
	private Pattern transferDonePattern;
	private Pattern taskIdPattern;
	
	public HadoopLogParser() {
		startupIpPattern = Pattern.compile(STARTUP_IP_TEMPLATE);
		jobSubmittedPattern = Pattern.compile(JOB_SUBMITTED_TEMPLATE);
		jobStartedPattern = Pattern.compile(JOB_STARTED_TEMPLATE);
		jobEndedPattern = Pattern.compile(JOB_ENDED_TEMPLATE);
		taskSubmittedPattern = Pattern.compile(TASK_SUBMITTED_TEMPLATE);
		taskReceivedPattern = Pattern.compile(TASK_RECEIVED_TEMPLATE);
		taskStartedPattern = Pattern.compile(TASK_STARTED_TEMPLATE);
		taskDonePattern = Pattern.compile(TASK_DONE_TEMPLATE);
		transferDonePattern = Pattern.compile(TRANSFER_DONE_TEMPLATE);

		taskIdPattern = Pattern.compile(TASK_ID_TEMPLATE);
	}
	
	public LogEntry parse(String line, LogEntry output) {
		output.clear();
		Matcher m = null;
		
		// We only need this once, so we might as well skip the matching when not necessary
		if(myIp == null) {
			m = startupIpPattern.matcher(line);
			if(m.find()) {
				myHost = m.group(1);
				myIp = m.group(2);
				System.out.println("My IP is " + myIp);
			}
		}
		
		// Job submitted
		m = jobSubmittedPattern.matcher(line);
		if(m.find()) {
			output.jobId = m.group(1);
			output.entryType = EntryType.JOB_SUBMITTED;
			
			return output;
		}
		
		// Job started
		m = jobStartedPattern.matcher(line);
		if(m.find()) {
			output.jobId = m.group(1);
			output.entryType = EntryType.JOB_STARTED;
			
			return output;
		}
		
		// Job ended
		m = jobEndedPattern.matcher(line);
		if(m.find()) {
			output.jobId = m.group(1);
			output.entryType = EntryType.JOB_DONE;
			
			return output;
		}
		
		// Task submitted
		m = taskSubmittedPattern.matcher(line);
		if(m.find()) {
			output.taskId = m.group(2);
			output.taskSubId = parseTaskSubId(output.taskId);
			output.jobId = parseJobId(output.taskId);
			
			// entryType defaults to "NONE"
			if(m.group(1).equals("MAP"))
				output.entryType = EntryType.MAP_SUBMITTED;
			else if(m.group(1).equals("REDUCE"))
				output.entryType = EntryType.REDUCE_SUBMITTED;
		}
		
		// Task received
		m = taskReceivedPattern.matcher(line);
		if(m.find()) {
			output.taskId = m.group(1);
			output.taskSubId = parseTaskSubId(output.taskId);
			output.jobId = parseJobId(output.taskId);

			if(parseTaskType(output.taskId) == TaskType.MAP)
				output.entryType = EntryType.MAP_RECEIVED;
			else
				output.entryType = EntryType.REDUCE_RECEIVED;
			
			return output;
		}
		
		// Task started
		m = taskStartedPattern.matcher(line);
		if(m.find()) {
			output.taskId = m.group(2);
			output.taskSubId = parseTaskSubId(output.taskId);
			output.jobId = parseJobId(output.taskId);

			if(parseTaskType(output.taskId) == TaskType.MAP)
				output.entryType = EntryType.MAP_STARTED;
			else
				output.entryType = EntryType.REDUCE_STARTED;
			
			return output;
		}
		
		// Task done
		m = taskDonePattern.matcher(line); 
		if(m.find()) {
			output.taskId = m.group(1);
			output.taskSubId = parseTaskSubId(output.taskId);
			output.jobId = parseJobId(output.taskId);
			
			if(parseTaskType(output.taskId) == TaskType.MAP)
				output.entryType = EntryType.MAP_DONE;
			else
				output.entryType = EntryType.REDUCE_DONE;
			
			return output;
		}
		
		// Transfer done
		m = transferDonePattern.matcher(line);
		if(m.find()) {
			output.taskId = m.group(6);
			output.taskSubId = parseTaskSubId(output.taskId);
			output.jobId = parseJobId(output.taskId);
			
			if(parseTaskType(output.taskId) == TaskType.MAP)
				output.entryType = EntryType.MAP_OUTPUT_SENT;
			else
				System.err.println("Unknown transfer type for task " + output.taskId);
			
			return output;
		}
		
		return output;
	}
	
	public LogEntry parse(String line) {
		return parse(line, new LogEntry());
	}
	
	private TaskType parseTaskType(String taskId) {
		Matcher m = taskIdPattern.matcher(taskId);
		if(m.find())
			return m.group(3).equals("m")? TaskType.MAP:TaskType.REDUCE;
		
		return null;
	}
	
	private String parseJobId(String taskId) {
		Matcher m = taskIdPattern.matcher(taskId);
		if(m.find())
			return m.group(1) + '_' + m.group(2);
		
		return null;
	}
	
	private String parseTaskSubId(String taskId) {
		Matcher m = taskIdPattern.matcher(taskId);
		if(m.find())
			return m.group(4);
		
		return null;
	}
	
	enum EntryType {
		JOB_SUBMITTED,
		JOB_STARTED,
		JOB_DONE,
		MAP_SUBMITTED,
		MAP_RECEIVED,
		MAP_STARTED,
		MAP_DONE,
		REDUCE_SUBMITTED,
		REDUCE_RECEIVED,
		MAP_OUTPUT_SENT,
		REDUCE_STARTED,
		REDUCE_DONE,
		
		NONE;
	}
	
	enum TaskType {
		MAP,
		REDUCE,
		SHUFFLE
	}
	
	class LogEntry {
		public String jobId;
		public String taskId;
		public String taskSubId;
		public String path;
		public EntryType entryType;
		
		public void clear() {
			jobId = taskId = taskSubId = path = null;
			entryType = EntryType.NONE;
		}
	}
}
