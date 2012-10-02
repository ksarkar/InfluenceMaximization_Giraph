package edu.ame.socialcascade.seedselection.Random;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

import edu.ame.socialcascade.model.Model;
import edu.ame.socialcascade.seedselection.Result;
import edu.ame.socialcascade.seedselection.greedy.Greedy;

public class RandomSeedSelection extends Greedy {
	
	private Random random = new Random(System.currentTimeMillis());

	public RandomSeedSelection(Model model, int numSeeds, int numRuns,
			String localSeedSetFile, String hdfsNodeListFile) {
		super(model, numSeeds, numRuns, localSeedSetFile, hdfsNodeListFile);
	}

	public RandomSeedSelection(Model model, int numSeeds, int numRuns) {
		super(model, numSeeds, numRuns);
	}

	@Override
	public Result run(String[] args) throws Exception {
		
		super.model.initModel(args);
		long numNodes = super.makeNodeList(args);
		System.out.println("number of nodes: " + numNodes);
		
		Set<Long> randomNodes = new HashSet<Long>();
		while (randomNodes.size() < super.numSeeds) {
			Long l = nextLong(numNodes);
			randomNodes.add(l);
			System.out.println("Next node id: " + l);
		}
		
		PriorityQueue<Long> randomNodespq = new PriorityQueue<Long>(randomNodes);
		
		Set<String> seedSet = new HashSet<String>();
		NodeList nl = new NodeList(super.hdfsNodeListFile);
		
		long i = 0;
		StringBuilder nextNode = new StringBuilder();
		while (nl.next(nextNode) && (randomNodespq.size() != 0)) {
			long next = randomNodespq.peek();
			if (i == next) {
				seedSet.add(nextNode.toString());
				randomNodespq.poll();
			}
			i++;
		}
		nl.close();
		
		super.writeSeedFile(seedSet);
		double sigma = simulation.run(args);
		
		return new Result(seedSet, sigma);
	}
	
	/**
	 * users the random method to return a long between 0(inclusive) and n(exclusive)
	 * 
	 * @param n 
	 */
	private long nextLong(long n) {
		 if (n <= 0)
		     throw new IllegalArgumentException("n must be positive");
		 
		 long bits, val;
		   do {
		      bits = (random.nextLong() << 1) >>> 1;
		      val = bits % n;
		   } while (bits-val+(n-1) < 0L);
		   return val; 
	}

}
