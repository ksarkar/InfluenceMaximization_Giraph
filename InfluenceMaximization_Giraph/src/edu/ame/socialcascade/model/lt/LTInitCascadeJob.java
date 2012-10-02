package edu.ame.socialcascade.model.lt;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;

import edu.ame.socialcascade.common.DefaultFilenames;
import edu.ame.socialcascade.util.Util;


public class LTInitCascadeJob implements Tool {

	private Configuration conf;
	
	public static class LTInitCascadeVertex extends EdgeListVertex<LongWritable,
															  LTVertexValueWritable,
															  FloatWritable,
															  FloatWritable>  {

	private static Random random = new Random(System.currentTimeMillis());
	
	@Override
	public void compute(Iterator<FloatWritable> arg0) throws IOException {
		
		LTVertexValueWritable vertexVal = this.getVertexValue();
		vertexVal.setThreshold(new DoubleWritable(random.nextDouble()));
		this.setVertexValue(vertexVal);
		
		this.voteToHalt();
	}

}
	
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		GiraphJob job = Util.getGiraphJob(this.getConf(), 
											 this.getClass(),
											 this.getClass().getName(), 
											 DefaultFilenames.HDFS_SIM, 
											 DefaultFilenames.HDFS_CASCADE_IN, 
											 LTInitCascadeVertex.class, 
											 null, 
											 null, 
											 LTModelVertexInputFormat.class, 
											 LTModelVertexOutputFormat.class, 
											 1);
		return job.run(true)? 0 : -1;
	}
	
}
