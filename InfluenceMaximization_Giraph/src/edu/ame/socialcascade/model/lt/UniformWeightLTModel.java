package edu.ame.socialcascade.model.lt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import edu.ame.socialcascade.model.AbstractModel;


/**
 * Main simlation class
 * @author kaushik
 *
 */

public class UniformWeightLTModel extends  AbstractModel {
	
	
    
    /** Class logger */
    //private static final Logger LOG =
      //  Logger.getLogger(UniformWeightLTModel.class);
	
	@Override
	public void initModel(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new UWLTInitModel(), args);
	}
	
	@Override
	public long runCascade(int numSeeds, String[] args) throws Exception {
		this.initCascade(args);
		return numSeeds + this.run(args);
	}
	
    public void initCascade(String[] args) throws Exception {
    	ToolRunner.run(new Configuration(), new LTInitCascadeJob(), args);
    }
    
    
    /**
     * Returns number of active users in addition to the initial seed set.
     * To get the spread need to add the initial seed set size, i.e. numSeeds + run(args);
     *  
     * @param args Hadoop generic arguments
     * @return Number of active users in addition to the initial seed set
     * @throws Exception
     */ 
    
    private long run(String[] args) throws Exception {
    	int spread = ToolRunner.run(new Configuration(), new UWLTCascadeRunnerJob(), args);
		if (spread == -1) {
			throw new Exception("job.run didn't return success!");
		}
		return spread;
    }
    
}
