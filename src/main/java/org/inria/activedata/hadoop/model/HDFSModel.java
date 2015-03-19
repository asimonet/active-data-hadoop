package org.inria.activedata.hadoop.model;

import org.inria.activedata.model.LifeCycleModel;
import org.inria.activedata.model.Place;
import org.inria.activedata.model.Transition;

/**
 * Data life cycle for a job in HDFS that can be connected to a Hadoop job.
 * 
 * @author Anthony SIMONET <anthony.simonet@inria.fr>
 */
public class HDFSModel extends LifeCycleModel {
	private static final long serialVersionUID = -3386201130127350029L;

	public HDFSModel() {
		super("HDFS");

		Place created = getStartPlace();
		Place terminated = getEndPlace();

		Transition delete = addTransition("delete");

		addArc(created, delete);
		addArc(delete, terminated);
		
		addCompositionTransition("create Hadoop job", created, new HadoopModel(this));
	}
}
