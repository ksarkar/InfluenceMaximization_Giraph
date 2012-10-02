package edu.ame.socialcascade.seedselection;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;

import edu.ame.socialcascade.model.Model;
import edu.ame.socialcascade.util.Util;

public class MakeNodeListJob implements Tool{
	
	Model model;
	String HDFSNodeListFilename;
	Configuration conf;

	public MakeNodeListJob(Model model, String HDFSNodeListFilename) {
		super();
		this.model = model;
		this.HDFSNodeListFilename = HDFSNodeListFilename;
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		JobConf conf = Util.getMapRedJobConf(this.getClass(),
											 this.getConf(),
											 this.getClass().getName(), 
											 TextInputFormat.class,
											 MakeNodeListMapper.class,
											 null,
											 null,
											 0, // no reducer
											 null, 
											 Text.class,
											 NullWritable.class,
											 SequenceFileOutputFormat.class,
											 model.getInputDirName(), 
											 this.HDFSNodeListFilename);
		
		Counters counters = JobClient.runJob(conf).getCounters();
		return (int)counters.findCounter(Task.Counter.MAP_INPUT_RECORDS).getCounter();
	}

	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;		
	}
}
