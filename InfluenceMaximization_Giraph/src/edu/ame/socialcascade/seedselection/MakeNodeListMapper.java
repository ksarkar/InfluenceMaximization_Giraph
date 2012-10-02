package edu.ame.socialcascade.seedselection;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.JSONArray;
import org.json.JSONException;

public class MakeNodeListMapper extends MapReduceBase 
					implements Mapper<LongWritable,
									  Text,
									  Text,
									  NullWritable>{

	@Override
	public void map(LongWritable key, 
					Text value,
					OutputCollector<Text, NullWritable> out, 
					Reporter rep) throws IOException {
		
		String line = value.toString();
		
		try {
			JSONArray jsonVertex = new JSONArray(line);
			Long vertexId = jsonVertex.getLong(0);
			out.collect(new Text(vertexId.toString()), NullWritable.get());
		} catch (JSONException e) {
			throw new IllegalArgumentException(
                    "next: Couldn't get vertex from line " + line, e);
		}
	}

}
