package edu.ame.socialcascade.test;

import java.util.Iterator;

import edu.ame.socialcascade.model.Model;
import edu.ame.socialcascade.model.ic.RandomProbICModel;
import edu.ame.socialcascade.model.lt.UniformWeightLTModel;
import edu.ame.socialcascade.seedselection.Result;
import edu.ame.socialcascade.seedselection.SeedSelectionStrategy;
import edu.ame.socialcascade.seedselection.Random.RandomSeedSelection;

public class RandomSeedSelTest {
	public static void main(String[] args) throws Exception {
		runUWLT(args);
		runRPIC(args);
	}
	
	private static void runUWLT(String[] args) throws Exception {
		Model model = new UniformWeightLTModel();
		SeedSelectionStrategy random = new RandomSeedSelection(model, 2, 2);
		
		long startTime = System.currentTimeMillis();
		Result result = random.run(args);
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
		SeedSelectionStrategy random = new RandomSeedSelection(model, 2, 2);
		
		long startTime = System.currentTimeMillis();
		Result result = random.run(args);
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
