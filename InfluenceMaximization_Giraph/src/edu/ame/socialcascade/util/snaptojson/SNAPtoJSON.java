package edu.ame.socialcascade.util.snaptojson;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.JSONArray;


import edu.ame.socialcascade.common.DefaultFilenames;
import edu.ame.socialcascade.util.Util;

public class SNAPtoJSON implements Tool {
	private Configuration conf;
	
	@Override
	public Configuration getConf() {
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public int run(String[] args) throws Exception {
	
		String usage = new String("Usage: SNAPtoJSON <inDir>");
		
		if (1 != args.length) {
			System.out.println(usage);
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		String inputDir = args[0];
		
		
		//String inputDir = "/in/tiny";
		String outputDir = DefaultFilenames.HDFS_INPUT;
		
		JobConf conf = Util.getMapRedJobConf(this.getClass(), 
											 this.getConf(), 
											 "SNAP-to-JSON", 
											 TextInputFormat.class, 
											 SNAPtoJSONMapper.class, 
											 Text.class, 
											 Text.class, 
											 1, 
											 SNAPtoJSONReducer.class, 
											 NullWritable.class, 
											 Text.class, 
											 TextOutputFormat.class, 
											 inputDir, 
											 outputDir);
		JobClient.runJob(conf);
											 
		return 0;
	}
	
	public static class SNAPtoJSONMapper extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, 
						Text value,
						OutputCollector<Text, Text> output, 
						Reporter reporter) throws IOException {
			
			String line = value.toString();
			
			if (line.charAt(0)!= '#'){ // check if it is a comment line
				String[] nodes = line.split("\\s");
				output.collect(new Text(nodes[0]), new Text(nodes[1]));
			}
		}
	}
	
	public static class SNAPtoJSONReducer extends MapReduceBase
		implements Reducer<Text, Text, NullWritable, Text> {

		public void reduce(Text key, 
						   Iterator<Text> values,
						   OutputCollector<NullWritable, Text> output, 
						   Reporter reporter) throws IOException {
	
			JSONArray jsonVertex = new JSONArray();
			
			jsonVertex.put(Integer.parseInt(key.toString()));
			
			JSONArray jsonEdges = new JSONArray();
			while (values.hasNext()) {
				jsonEdges.put(Integer.parseInt(values.next().toString()));
			}
			
			jsonVertex.put(jsonEdges);
	
			output.collect(null, new Text(jsonVertex.toString()));
		}
	
	}
	
	public static void main(String[] args) {
		int res = 0;
		long startTime = System.currentTimeMillis();
		try {
			res = ToolRunner.run(new Configuration(), new SNAPtoJSON(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Job Finished in "+
                (System.currentTimeMillis() - startTime)/1000.0 + " seconds");
		
		System.exit(res);
	}

}
