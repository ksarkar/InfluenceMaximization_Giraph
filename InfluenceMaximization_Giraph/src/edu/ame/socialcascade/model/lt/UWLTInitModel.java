package edu.ame.socialcascade.model.lt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.Tool;

import edu.ame.socialcascade.common.DefaultFilenames;
import edu.ame.socialcascade.common.InputReaderVertexInputFormat;
import edu.ame.socialcascade.util.Util;

public class UWLTInitModel implements Tool {
	
	/** Class logger */
    //private static final Logger LOG = Logger.getLogger(UWLTInitModel.class);
    
    private Configuration conf;
	
	public static class UWLTMessageWritable implements Writable {
		private LongWritable vertexId;
		private FloatWritable weight;
		
		public UWLTMessageWritable() {
			super();
		}

		public UWLTMessageWritable(LongWritable vertexId, FloatWritable weight) {
			super();
			this.vertexId = vertexId;
			this.weight = weight;
		}

		public LongWritable getVertexId() {
			return vertexId;
		}

		public void setVertexId(LongWritable vertexId) {
			this.vertexId = vertexId;
		}

		public FloatWritable getWeight() {
			return weight;
		}

		public void setWeight(FloatWritable weight) {
			this.weight = weight;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.vertexId = new LongWritable();
			this.vertexId.readFields(in);
			
			this.weight = new FloatWritable();
			this.weight.readFields(in);			
		}

		@Override
		public void write(DataOutput out) throws IOException {
			this.vertexId.write(out);
			this.weight.write(out);
			
		}
		
	}
	
	public static class UWLTInitModelVertex extends 
							EdgeListVertex<LongWritable,
											LTVertexValueWritable,
											FloatWritable,
											UWLTMessageWritable> {

		@Override
		public void compute(Iterator<UWLTMessageWritable> msgItr) throws IOException {
			
			if (this.getSuperstep() == 0) {
				float weight = (float) 1 / this.getNumOutEdges();
				UWLTMessageWritable message = new UWLTMessageWritable(this.getVertexId(),
																	new FloatWritable(weight));
				this.sendMsgToAllEdges(message);
				// LOG.info("VertexId: " + this.getVertexId() + "; in superstep 0.");
			}
			
			else if (this.getSuperstep() == 1) {
				while (msgItr.hasNext()) {
					UWLTMessageWritable message = msgItr.next();
					LongWritable edgeId = message.getVertexId();
					if (this.hasEdge(edgeId)) {
						this.removeEdge(edgeId);
					}
					this.addEdge(edgeId, message.getWeight());
					//LOG.info("VertexId: " + this.getVertexId() + "; added edge to " + 
						//	edgeId.get() + ", in superstep 1");
				}
				
				this.setVertexValue(new LTVertexValueWritable(new BooleanWritable(false),
															  new DoubleWritable(0.0d)));
			}
			
			else {
				//LOG.info("VertexId: " + this.getVertexId() + "; Superstep: " +
					//	this.getSuperstep() +"; now halting.");
				
				this.voteToHalt();
			}
		}
		
	}

	@Override
	public int run(String[] arg0) throws Exception {
		GiraphJob job = Util.getGiraphJob(this.getConf(), 
										 this.getClass(),
										 this.getClass().getName(), 
										 DefaultFilenames.HDFS_INPUT, 
										 DefaultFilenames.HDFS_MODEL, 
										 UWLTInitModelVertex.class, 
										 null, 
										 null, 
										 InputReaderVertexInputFormat.class, 
										 LTModelVertexOutputFormat.class, 
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
