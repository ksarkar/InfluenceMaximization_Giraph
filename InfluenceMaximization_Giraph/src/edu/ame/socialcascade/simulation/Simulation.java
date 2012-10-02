package edu.ame.socialcascade.simulation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import edu.ame.socialcascade.common.DefaultFilenames;
import edu.ame.socialcascade.model.Model;


public class Simulation {

	private Model model;
	private int numRuns;
	private String localSeedFile;	
	
	
	public Simulation(Model model) {
		this(model, 1, null);
	}

	public Simulation(Model model, int numRuns) {
		this(model, numRuns, null);
	}

	public Simulation(Model model, int numRuns, String localSeedFile) {
		super();
		this.model = model;
		this.numRuns = numRuns;
		this.localSeedFile = (localSeedFile == null)? DefaultFilenames.LOCAL_SEED_LIST : localSeedFile;
	}

	public double run(String localSeedFile, String[] args) throws Exception {
		if (localSeedFile != null) {
			this.localSeedFile = localSeedFile;
		}
		
		int numSeeds = this.initSimulation(args);
				
		long sum = 0;
		for (int i = 0; i < numRuns; i++) {
			sum = sum + model.runCascade(numSeeds, args);
		}

		return (double) sum / numRuns;
	}
	
	public double run(String[] args) throws Exception {
		return this.run(null, args);
	}
	
	public double singleRun(String[] args) throws Exception {
		model.initModel(args);
		return this.run(args);
	}
	
	private int initSimulation(String[] args) throws Exception {
		return ToolRunner.run(new Configuration(), new InitSimHadoopJob(this.localSeedFile), args);
	}
	
	public int getNumSeed() throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(this.localSeedFile));
		int seeds = 0;
		try {
			while (reader.readLine() != null) seeds++;
		} finally {
			reader.close();
		}
		
		return seeds;
	}
	
}
