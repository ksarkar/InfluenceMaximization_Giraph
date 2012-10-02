package edu.ame.socialcascade.model.ic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import edu.ame.socialcascade.model.AbstractModel;

public class RandomProbICModel extends AbstractModel {

	@Override
	public void initModel(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new RPICInitModelJob(), args);
	}

	@Override
	public long runCascade(int numSeeds, String[] args) throws Exception {
		this.initCascade(args);
		return numSeeds + this.run(args);
	}
	
	private void initCascade(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ICInitCascadeJob(), args);
	}
	
	private long run(String[] args) throws Exception {
		int spread = ToolRunner.run(new Configuration(), new RPICCascadeRunnerJob(), args);
		if (spread == -1) {
			throw new Exception("job.run didn't return success!");
		}
		return spread;
	}

}
