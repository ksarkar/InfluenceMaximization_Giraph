package edu.ame.socialcascade.simulation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import edu.ame.socialcascade.common.DefaultFilenames;
import edu.ame.socialcascade.util.Util;

public class InitSimHadoopJob implements Tool {
	
	public static enum SeedCounter {SEED_COUNT};
	
	
	private Configuration conf;
	private String localSeedFilename;
	
	
	
	public InitSimHadoopJob(String localSeedFilename) {
		super();
		this.localSeedFilename = localSeedFilename;
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;			
	}
	
	
	private void cacheSeedList(JobConf conf) throws IOException {
	    FileSystem fs = FileSystem.get(conf);
	    Path hdfsPath = new Path(DefaultFilenames.LOCAL_SEED_LIST);
	    Path localPath = new Path(this.localSeedFilename);

	    // upload the file to hdfs. Overwrite any existing copy.
	    fs.copyFromLocalFile(false, true, localPath, hdfsPath);

	    DistributedCache.addCacheFile(hdfsPath.toUri(), conf);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		JobConf conf = Util.getMapRedJobConf(this.getClass(),
											 this.getConf(),
											 this.getClass().getName(), 
											 TextInputFormat.class,
											 InitSimMapper.class,
											 null,
											 null,
											 0, // no reducer
											 null, 
											 NullWritable.class,
											 Text.class,
											 TextOutputFormat.class,
											 DefaultFilenames.HDFS_MODEL, 
											 DefaultFilenames.HDFS_SIM);

		this.cacheSeedList(conf);
		Counters counters = JobClient.runJob(conf).getCounters();
		return (int)counters.getCounter(SeedCounter.SEED_COUNT);

	}

}
