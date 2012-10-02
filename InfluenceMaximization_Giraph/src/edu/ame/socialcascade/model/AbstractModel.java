package edu.ame.socialcascade.model;

import edu.ame.socialcascade.common.DefaultFilenames;
import edu.ame.socialcascade.model.Model;

public abstract class AbstractModel implements Model {

	@Override
	public String getInputDirName() {
		return DefaultFilenames.HDFS_INPUT;
	}

	@Override
	public abstract void initModel(String[] args) throws Exception;

	@Override
	public abstract long runCascade(int numSeeds, String[] args) throws Exception;

}
