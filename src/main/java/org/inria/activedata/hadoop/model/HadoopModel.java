package org.inria.activedata.hadoop.model;

import org.inria.activedata.model.LifeCycleModel;
import org.inria.activedata.model.Place;
import org.inria.activedata.model.Transition;

/**
 * Life cycle model for a Hadoop job.
 * 
 * @author Anthony SIMONET <anthony.simonet@inria.fr>
 */
public class HadoopModel extends LifeCycleModel {
	private static final long serialVersionUID = -5778242702184644394L;

	public HadoopModel() {
		this(null);
	}
	
	public HadoopModel(LifeCycleModel hdfsModel) {
		super("Hadoop");
		
		// Places
		Place created = getStartPlace();
		Place terminated = getEndPlace();
		
		Place jobSubmitted = addPlace("Job submitted");
		Place jobStarted = addPlace("Job started");
		Place jobDone = addPlace("Job done");
		Place mapSubmitted = addPlace("Map submitted");
		Place mapReceived = addPlace("Map received");
		Place mapStarted = addPlace("Map started");
		Place mapOutputSent = addPlace("Map output sent");
		Place reduceSubmitted = addPlace("Reduce submitted");
		Place reduceReceived = addPlace("Reduce received");
		Place reduceStarted = addPlace("Reduce started");
		
		// Transitions
		Transition submitJob = addTransition("Submit job");
		Transition startJob = addTransition("Start job");
		Transition endJob = addTransition("End job");
		Transition submitMap = addTransition("Submit map");
		Transition assignMap = addTransition("Assign map");
		Transition startMap = addTransition("Start map");
		Transition endMap = addTransition("End map");
		Transition shuffle = addTransition("Shuffle");
		Transition submitReduce = addTransition("Submit reduce");
		Transition assignReduce = addTransition("Assign reduce");
		Transition startReduce = addTransition("Start reduce");
		Transition endReduce = addTransition("End reduce");
		Transition removeJob = addTransition("Remove job");
		
		// Arcs
		addArc(created, submitJob);
		addArc(submitJob, jobSubmitted);
		addArc(jobSubmitted, startJob);
		addArc(startJob, jobStarted);
		addArc(jobStarted, endJob);
		addArc(endJob, jobDone);
		addArc(jobStarted, submitMap);
		addArc(submitMap, jobStarted);
		addArc(submitMap, mapSubmitted);
		addArc(mapSubmitted, assignMap);
		addArc(assignMap, mapReceived);
		addArc(mapReceived, startMap);
		addArc(startMap, mapStarted);
		addArc(mapStarted, endMap);
		addArc(mapStarted, shuffle);
		addArc(shuffle, mapStarted);
		addArc(shuffle, mapOutputSent);
		addArc(jobStarted, submitReduce);
		addArc(submitReduce, jobStarted);
		addArc(submitReduce, reduceSubmitted);
		addArc(reduceSubmitted, assignReduce);
		addArc(assignReduce, reduceReceived);
		addArc(reduceReceived, startReduce);
		addArc(mapOutputSent, startReduce);
		addArc(startReduce, reduceStarted);
		addArc(reduceStarted, endReduce);
		addArc(jobDone, removeJob);
		addArc(removeJob, terminated);
		
		if(hdfsModel != null)
			addCompositionTransition("Derive", jobDone, hdfsModel);
	}
}
