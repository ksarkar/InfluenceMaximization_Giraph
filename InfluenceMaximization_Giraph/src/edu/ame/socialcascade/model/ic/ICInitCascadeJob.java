package edu.ame.socialcascade.model.ic;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import org.apache.giraph.graph.EdgeListVertex; 
import org.apache.giraph.graph.GiraphJob;

import edu.ame.socialcascade.common.DefaultFilenames;
import edu.ame.socialcascade.util.Util;

public class ICInitCascadeJob implements Tool {
	
	private Configuration conf;
	
	public static class ICInitCascadeVertex extends EdgeListVertex<LongWritable,
																	ICVertexValueWritable,
																	DoubleWritable,
																	BooleanWritable> {

		@Override
		public void compute(Iterator<BooleanWritable> arg0) throws IOException {
			ICVertexValueWritable vertexVal = this.getVertexValue();
			boolean active = vertexVal.getIsActive().get();
			if (active) {
				vertexVal.getIsContagious().set(true);
				this.setVertexValue(vertexVal);
			}
			this.voteToHalt();
		}
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		GiraphJob job = Util.getGiraphJob(this.getConf(), 
										 this.getClass(),
										 this.getClass().getName(), 
										 DefaultFilenames.HDFS_SIM, 
										 DefaultFilenames.HDFS_CASCADE_IN, 
										 ICInitCascadeVertex.class, 
										 null, 
										 null, 
										 ICModelVertexInputFormat.class, 
										 ICModelVertexOutputFormat.class, 
										 1);
		return job.run(true)? 0 : -1;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}
