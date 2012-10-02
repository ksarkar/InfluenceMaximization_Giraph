package edu.ame.socialcascade.simulation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.json.JSONArray;
import org.json.JSONException;

import edu.ame.socialcascade.simulation.InitSimHadoopJob.SeedCounter;
import edu.ame.socialcascade.common.DefaultFilenames;

public class InitSimMapper extends MapReduceBase
	implements Mapper<LongWritable, Text, NullWritable, Text>{
	
	private Set<String> seeds = null;
	
	public void configure(JobConf conf) {
	    try {
	      String seedCacheName = new Path(DefaultFilenames.HDFS_SEED_LIST).getName();
	      Path [] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
	      if (null != cacheFiles && cacheFiles.length > 0) {
	        for (Path cachePath : cacheFiles) {
	          if (cachePath.getName().equals(seedCacheName)) {
	            loadSeedList(cachePath);
	            break;
	          }
	        }
	      }
	    } catch (IOException ioe) {
	      System.err.println("IOException reading from distributed cache");
	      System.err.println(ioe.toString());
	    }
	  }

	@Override
	public void map(LongWritable key, 
					Text val,
					OutputCollector<NullWritable, Text> out, 
					Reporter reporter) throws IOException {
		try {
			JSONArray jsonVertex = new JSONArray(val.toString());
			String vertexId = new Long(jsonVertex.getLong(0)).toString();
			if (seeds.contains(vertexId)) {
				 reporter.incrCounter(SeedCounter.SEED_COUNT, 1L);
				 
				 JSONArray jsonVertexVal = jsonVertex.getJSONArray(1);
				 JSONArray jsonThreshold = jsonVertexVal.getJSONArray(1);
				
				 JSONArray jsonEdges = jsonVertex.getJSONArray(2);
				 
				 JSONArray jsonNewVertexVal = new JSONArray();
				 jsonNewVertexVal.put(true);
				 jsonNewVertexVal.put(jsonThreshold);
				 
				 JSONArray jsonNewVertex = new JSONArray();
				 jsonNewVertex.put(Long.parseLong(vertexId));
				 jsonNewVertex.put(jsonNewVertexVal);
				 jsonNewVertex.put(jsonEdges);
				 
				 out.collect(null, new Text(jsonNewVertex.toString()));
			}
			else {
				out.collect(null, val);
			}
		} catch (JSONException e) {
			throw new IllegalArgumentException("map: cannot get vertex from line " + val.toString(), e);
		}
		
		
	}
	
	void loadSeedList(Path cachePath) throws IOException {
	    // note use of regular java.io methods here - this is a local file now
	    BufferedReader wordReader = new BufferedReader(
	        new FileReader(cachePath.toString()));
	    try {
	      String line;
	      this.seeds = new HashSet<String>();
	      while ((line = wordReader.readLine()) != null) {
	        this.seeds.add(line);
	      }
	    } finally {
	      wordReader.close();
	    }
	  }

}
