package edu.ame.socialcascade.test;

import java.util.Iterator;

import edu.ame.socialcascade.model.Model;
import edu.ame.socialcascade.model.lt.UniformWeightLTModel;
import edu.ame.socialcascade.seedselection.Result;
import edu.ame.socialcascade.seedselection.SeedSelectionStrategy;
import edu.ame.socialcascade.seedselection.greedy.Greedy;
import edu.ame.socialcascade.simulation.Simulation;

public class UWLTTest {
	public static void main(String[] args) throws Exception {
		
		runCascade(args);
		//runSimulation(args);
		//runOptimization(args); 
		
	}
	
	private static void runCascade(String[] args) throws Exception {
		Model model = new UniformWeightLTModel();
		Simulation sim = new Simulation(model, 1);
		
		long startTime = System.currentTimeMillis();
		System.out.println("Spread: " +  sim.singleRun(args));
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
	}
	
	private static void runSimulation(String[] args) throws Exception {
		Model model = new UniformWeightLTModel();
		Simulation sim = new Simulation(model, 5);
		
		long startTime = System.currentTimeMillis();
		System.out.println("Spread: " +  sim.singleRun(args));
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
	}
	
	public static void runOptimization(String[] args) throws Exception {
		Model model = new UniformWeightLTModel();
		SeedSelectionStrategy greedy = new Greedy(model, 1, 2);
		
		long startTime = System.currentTimeMillis();
		Result result = greedy.run(args);
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
		System.out.println("Seed set for maximizing contagion: ");
		Iterator<String> seeds = result.getSeedSet().iterator();
		while (seeds.hasNext()) {
			System.out.println(seeds.next());
		}
		System.out.println("Maximum achievable spread: " + result.getSpread());
	}


}
