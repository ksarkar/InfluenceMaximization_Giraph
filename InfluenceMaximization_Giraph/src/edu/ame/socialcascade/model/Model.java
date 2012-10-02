package edu.ame.socialcascade.model;

public interface Model {
	public void initModel(String[] args) throws Exception;
	public long runCascade(int numSeeds, String[] args) throws Exception;
	public String getInputDirName();
}
