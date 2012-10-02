package edu.ame.socialcascade.util;

import java.io.IOException;

import org.apache.giraph.graph.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Util {
	
	public static GiraphJob getGiraphJob(Configuration conf,
										 Class<?> JobClass,
										 String jobName,
										 String inDir,
										 String outDir,
										 Class<?> vertexClass,
										 Class<?> masterComputeClass,
										 Class<?> workerContextClass,
										 Class<?> vertexInputFormatClass,
										 Class<?> vertexOutputFormatClass,
										 int numWorker) throws IOException {
	
		GiraphJob job = new GiraphJob(conf, jobName);
		
		if (JobClass != null) {
			job.getInternalJob().setJarByClass(JobClass);
		}
		
		job.setVertexClass(vertexClass);
		if (masterComputeClass != null) {
			job.setMasterComputeClass(masterComputeClass);
		}
		if (workerContextClass != null) {
			job.setWorkerContextClass(workerContextClass);
		}
		job.setVertexInputFormatClass(vertexInputFormatClass);
		job.setVertexOutputFormatClass(vertexOutputFormatClass);
		job.setWorkerConfiguration(numWorker, numWorker, 100.0f);
		
		// delete the existing target output folder
		FileSystem fs = FileSystem.get(job.getConfiguration());
		fs.delete(new Path(outDir), true);
		
		
		// specify input and output DIRECTORIES (not files)
		FileInputFormat.addInputPath(job.getInternalJob(), new Path(inDir));
		FileOutputFormat.setOutputPath(job.getInternalJob(), new Path(outDir));
		
		return job;
	}
	
	public static JobConf getMapRedJobConf(Class<?> JobClass,
										   Configuration Conf,
										   String jobName,
		   	  							   Class<? extends InputFormat> inputFormatClass,
		   	  							   Class<? extends Mapper> mapperClass,
		   	  							   Class<?> mapOutputKeyClass,
		   	  							   Class<?> mapOutputValueClass,
		   	  							   int numReducer,
		   	  							   Class<? extends Reducer> reducerClass,
		   	  							   Class<?> outputKeyClass,
		   	  							   Class<?> outputValueClass,
		   	  							   Class<? extends OutputFormat> outputFormatClass,
		   	  							   String inputDir,
		   	  							   String outputDir) throws IOException {

		JobConf conf = null;
		if (JobClass == null) {
			conf = new JobConf();
		}
		else {
			conf = new JobConf(Conf, JobClass);
		}
		
		if (jobName != null)
			conf.setJobName(jobName);
		
		conf.setInputFormat(inputFormatClass);
		
		conf.setMapperClass(mapperClass);
		
		if (numReducer == 0) {
			conf.setNumReduceTasks(0);
			
			conf.setOutputKeyClass(outputKeyClass);
			conf.setOutputValueClass(outputValueClass);
			
			conf.setOutputFormat(outputFormatClass);
		
		} else {
			// may set actual number of reducers
			conf.setNumReduceTasks(numReducer);
			
			conf.setMapOutputKeyClass(mapOutputKeyClass);
			conf.setMapOutputValueClass(mapOutputValueClass);
			
			conf.setReducerClass(reducerClass);
			
			conf.setOutputKeyClass(outputKeyClass);
			conf.setOutputValueClass(outputValueClass);
			
			conf.setOutputFormat(outputFormatClass);
			
		}
		
		// delete the existing target output folder
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(outputDir), true);
		
		
		// specify input and output DIRECTORIES (not files)
		org.apache.hadoop.mapred.FileInputFormat.addInputPath(conf, new Path(inputDir));
		org.apache.hadoop.mapred.FileOutputFormat.setOutputPath(conf, new Path(outputDir));
		
		return conf;		

}
	
}
