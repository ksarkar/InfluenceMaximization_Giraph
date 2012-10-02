package edu.ame.socialcascade.model.lt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.graph.MasterCompute;
import org.apache.giraph.graph.WorkerContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;

import edu.ame.socialcascade.common.DefaultFilenames;
import edu.ame.socialcascade.util.Util;


public class UWLTCascadeRunnerJob implements Tool {
	
	/** Configuration */
    private Configuration conf;   
    
    public static enum ActiveCounter {ACTIVATED_COUNT};
    public static final String ROUND_ACTIVE = "round.activated";

	@Override
	public Configuration getConf() {
		return this.conf;
	}
	

	public static class UWLTCascadeVertex extends 
		EdgeListVertex<LongWritable, LTVertexValueWritable, 
						FloatWritable, FloatWritable> {

		@Override
		public void compute(Iterator<FloatWritable> msgItr) throws IOException {
			
			LTVertexValueWritable vertexValue = this.getVertexValue();
			boolean active = vertexValue.getIsActive().get();
			
			
			//LOG.info("Vertex " + this.getVertexId() + 
					//" in superstep " + this.getSuperstep());
		
			
			if (!active) { // check if total weight is more than threshold
				double sum = 0.0d;
				while (msgItr.hasNext()) {
					sum += msgItr.next().get(); 
				}
				
				if (sum >= this.getVertexValue().getThreshold().get()) {
					active = true;
					LongSumAggregator sumAg = (LongSumAggregator) 
								this.getAggregator(UWLTCascadeRunnerJob.ROUND_ACTIVE);
					sumAg.aggregate(1L);
					vertexValue.setIsActive(new BooleanWritable(true));
					this.setVertexValue(vertexValue);
					
					//LOG.info("Vertex " + this.getVertexId() + 
						//	" turned active in superstep " + this.getSuperstep());	
				}
			}
			
			if (active) {
				Iterator<LongWritable> edges = this.getOutEdgesIterator();
				while (edges.hasNext()) {
					LongWritable edge = edges.next();
					this.sendMsg(edge, this.getEdgeValue(edge));
				}
			}
		}
		
	}
	
	public static class UWLTCascadeMasterCompute extends MasterCompute {
		
		private long activeSetSize = 0;

		@Override
		public void initialize() throws InstantiationException,
				IllegalAccessException {
			
			this.registerAggregator(UWLTCascadeRunnerJob.ROUND_ACTIVE, 
									LongSumAggregator.class);
			
			//LOG.info("MasterCompute#initialize." );
			
		}
		
		@Override
		public void compute() {
			
			LongSumAggregator roundAg = (LongSumAggregator) this.getAggregator(
												UWLTCascadeRunnerJob.ROUND_ACTIVE);
			
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
					
					this.haltComputation();
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
	
	public static class CascadeWorkerContext extends WorkerContext {
		
		@Override
		public void preApplication() throws InstantiationException,
				IllegalAccessException {
			this.registerAggregator(UWLTCascadeRunnerJob.ROUND_ACTIVE, 
									LongSumAggregator.class);

			//LOG.info("WorkerContext#preApplication: registering the round aggregator");
		}
		
		@Override
		public void preSuperstep() {			
			this.useAggregator(UWLTCascadeRunnerJob.ROUND_ACTIVE);
			
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
	public void setConf(Configuration conf) {
		this.conf = conf;
		
	}

	@Override
	public int run(String[] args) throws Exception {
		
		GiraphJob job = Util.getGiraphJob(this.getConf(),
									 this.getClass(),
									 this.getClass().getName(),
									 DefaultFilenames.HDFS_CASCADE_IN,
									 DefaultFilenames.HDFS_CASCADE_OUT,
									 UWLTCascadeVertex.class,
									 UWLTCascadeMasterCompute.class,
									 CascadeWorkerContext.class,
									 LTModelVertexInputFormat.class,
									 LTModelVertexOutputFormat.class,
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
}	
