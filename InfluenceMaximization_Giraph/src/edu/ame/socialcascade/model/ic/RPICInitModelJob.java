package edu.ame.socialcascade.model.ic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import edu.ame.socialcascade.common.DefaultFilenames;
import edu.ame.socialcascade.common.InputReaderVertexInputFormat;
import edu.ame.socialcascade.util.Util;

public class RPICInitModelJob implements Tool {
	Configuration conf;
	
	public static class RPICInitModelVertex extends EdgeListVertex<LongWritable,
																	ICVertexValueWritable,
																	DoubleWritable,
																	BooleanWritable>{
		
		private static Random random = new Random(System.currentTimeMillis());

		@Override
		public void compute(Iterator<BooleanWritable> arg0) throws IOException {
			
			ICVertexValueWritable vertexValue = new ICVertexValueWritable(new BooleanWritable(false),
																		  new BooleanWritable(false));
			this.setVertexValue(vertexValue);
			
			Iterator<LongWritable> edges = this.getOutEdgesIterator();
			ArrayList<LongWritable> edgeList = new ArrayList<LongWritable>();
			while(edges.hasNext()) {
				edgeList.add(edges.next());				
			}
			 
			edges = edgeList.iterator();
			while (edges.hasNext()) {
				LongWritable edgeId = edges.next();
				
				if (this.hasEdge(edgeId)) {
					this.removeEdge(edgeId);
				}
				this.addEdge(edgeId, new DoubleWritable(random.nextDouble()));
			}
			
			this.voteToHalt();			
		}
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		GiraphJob job = Util.getGiraphJob(this.getConf(), 
										 this.getClass(),
										 this.getClass().getName(), 
										 DefaultFilenames.HDFS_INPUT, 
										 DefaultFilenames.HDFS_MODEL, 
										 RPICInitModelVertex.class, 
										 null, 
										 null, 
										 InputReaderVertexInputFormat.class, 
										 ICModelVertexOutputFormat.class, 
										 1);

		return job.run(true)? 0 : -1;
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

}
