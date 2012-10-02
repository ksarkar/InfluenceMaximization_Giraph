package edu.ame.socialcascade.seedselection;

import java.util.Set;

public class Result {
	private Set<String> seedSet;
	private double spread;
	
	
	public Result(Set<String> seedSet, double spread) {
		super();
		this.seedSet = seedSet;
		this.spread = spread;
	}
	
	public Set<String> getSeedSet() {
		return seedSet;
	}
	public double getSpread() {
		return spread;
	}
	public void setSeedSet(Set<String> seedSet) {
		this.seedSet = seedSet;
	}
	public void setSpread(double spread) {
		this.spread = spread;
	}
	
	
}
