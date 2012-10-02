package edu.ame.socialcascade.model.ic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.MasterCompute;
import org.apache.giraph.graph.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;

import edu.ame.socialcascade.common.DefaultFilenames;
import edu.ame.socialcascade.util.Util;

public class RPICCascadeRunnerJob implements Tool {
	
	public static enum ActiveCounter {ACTIVATED_COUNT};
	public static final String ROUND_ACTIVE = "round.activated";
	
	//private static Logger LOG = Logger.getLogger(RPICCascadeRunnerJob.class);
	
	Configuration conf;
	
	
	public static class RPICModelVertex extends EdgeListVertex<LongWritable,
																	ICVertexValueWritable,
																	DoubleWritable,
																	BooleanWritable> {
		
		private static Random random = new Random(System.currentTimeMillis());

		@Override
		public void compute(Iterator<BooleanWritable> msgItr) throws IOException {
			
			ICVertexValueWritable vertexVal = this.getVertexValue();
			boolean active = vertexVal.getIsActive().get();
			boolean contagious = vertexVal.getIsContagious().get();
			
			if (!active) {
				while (msgItr.hasNext()) {
					if (msgItr.next().get() == true) {
						contagious = true;
						active = true;
						vertexVal.setIsActive(new BooleanWritable(true));	
						
						LongSumAggregator sumAg = (LongSumAggregator) 
									this.getAggregator(RPICCascadeRunnerJob.ROUND_ACTIVE);
						sumAg.aggregate(1L);
						break;
					}
				}
			}
			
			if (contagious) {
				Iterator<LongWritable> edges = this.getOutEdgesIterator();
				while (edges.hasNext()) {
					LongWritable edgeId = edges.next();
					if (random.nextDouble() <= this.getEdgeValue(edgeId).get()) {
						this.sendMsg(edgeId, new BooleanWritable(true));
					}
				}
				vertexVal.setIsContagious(new BooleanWritable(false));
			}
			
			this.setVertexValue(vertexVal);		
			this.voteToHalt();
		}
		
	}
	
	public static class RPICModelMasterCompute extends MasterCompute {
		
		private long activeSetSize = 0;

		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			
			this.registerAggregator(RPICCascadeRunnerJob.ROUND_ACTIVE, 
									LongSumAggregator.class);
			
			//LOG.info("MasterCompute#initialize." );
			
		}
		
		@Override
		public void compute() {
			
			LongSumAggregator roundAg = (LongSumAggregator) this.getAggregator(
									RPICCascadeRunnerJob.ROUND_ACTIVE);
			
			if (this.getSuperstep() <= 1) {
				roundAg.setAggregatedValue(0L);
				//LOG.info("MasterCompute#compute: round # " + this.getSuperstep() + 
					//	"; nothing happens as yet! ");
			}
			else {
				long roundTotal = roundAg.getAggregatedValue().get();
				if (roundTotal > 0) {
					activeSetSize += roundTotal;
					roundAg.setAggregatedValue(0L);
					//LOG.info("MasterCompute#compute: round # " + this.getSuperstep() +
						//	"; roundTot: " + roundTotal + "; total: " + this.activeSetSize +
							//"; go on to next round");
				}
				else {
					//LOG.info("MasterCompute#compute: round # " + this.getSuperstep() +
						//	"; total: " + this.activeSetSize +"; now halting the computation");
					
					this.getContext().getCounter(
							ActiveCounter.ACTIVATED_COUNT).increment(this.activeSetSize);
					
					//this.haltComputation();
				}
			}
			
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			
		}
		
	}
	
	public static class RPICModelWorkerContext extends WorkerContext {
		
		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
			this.registerAggregator(RPICCascadeRunnerJob.ROUND_ACTIVE, 
									LongSumAggregator.class);

			//LOG.info("WorkerContext#preApplication: registering the round aggregator");
		}
		
		@Override
		public void preSuperstep() {			
			this.useAggregator(RPICCascadeRunnerJob.ROUND_ACTIVE);
			
			//LOG.info("workercontext#preSuperstep: superstep # " + this.getSuperstep());
		}

		@Override
		public void postApplication() {
			//LOG.info("now workercontext#postApplication()");
		}

		@Override
		public void postSuperstep() {
			//LOG.info("WorkerContext#postSuperstep()");
		}

		
	}	

	@Override
	public int run(String[] arg0) throws Exception {
		GiraphJob job = Util.getGiraphJob(this.getConf(), 
										 this.getClass(),
										 this.getClass().getName(), 
										 DefaultFilenames.HDFS_CASCADE_IN, 
										 DefaultFilenames.HDFS_CASCADE_OUT, 
										 RPICModelVertex.class, 
										 RPICModelMasterCompute.class, 
										 RPICModelWorkerContext.class, 
										 ICModelVertexInputFormat.class, 
										 ICModelVertexOutputFormat.class, 
										 1);
		boolean success = job.run(true);
        if (success) {
        	return (int)job.getInternalJob().getCounters().findCounter(
        			ActiveCounter.ACTIVATED_COUNT).getValue();
        }
        else {
        	return -1;
        }
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
