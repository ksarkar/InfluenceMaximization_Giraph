package edu.ame.socialcascade.test;

import java.util.Iterator;

import edu.ame.socialcascade.model.Model;
import edu.ame.socialcascade.model.ic.RandomProbICModel;
import edu.ame.socialcascade.model.lt.UniformWeightLTModel;
import edu.ame.socialcascade.seedselection.Result;
import edu.ame.socialcascade.seedselection.SeedSelectionStrategy;
import edu.ame.socialcascade.seedselection.lazygreedy.LazyGreedy;

public class LazyGreedyTest {
	public static void main(String[] args) throws Exception {
		runUWLT(args);
		runRPIC(args);
	}
	
	private static void runUWLT(String[] args) throws Exception {
		Model model = new UniformWeightLTModel();
		SeedSelectionStrategy lazyGreedy = new LazyGreedy(model, 2, 2);
		
		long startTime = System.currentTimeMillis();
		Result result = lazyGreedy.run(args);
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
		System.out.println("Seed set for maximizing contagion: ");
		Iterator<String> seeds = result.getSeedSet().iterator();
		while (seeds.hasNext()) {
			System.out.println(seeds.next());
		}
		System.out.println("Maximum achievable spread: " + result.getSpread());
	}
	
	private static void  runRPIC(String[] args) throws Exception {
		Model model = new RandomProbICModel();
		SeedSelectionStrategy laztGreedy = new LazyGreedy(model, 2, 2);
		
		long startTime = System.currentTimeMillis();
		Result result = laztGreedy.run(args);
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
