package edu.ame.socialcascade.common;

import java.io.IOException;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Maps;

public class InputReaderVertexInputFormat<V1 extends Writable,
										  E1 extends Writable,
										  M1 extends Writable> 
				extends TextVertexInputFormat<LongWritable, V1, E1, M1> {

	@Override
	public VertexReader<LongWritable, V1, E1, M1> createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new InputReaderVertexReader<V1, E1, M1>(
	            textInputFormat.createRecordReader(split, context));
	}
	
    /**
     * VertexReader that supports initModel(). In this
     * case, the edge values are not used.  The files should be in the
     * following JSON format:
     * JSONArray(<vertex id>, JSONArray(<dest vertex id>, ...))
     * Here is an example with vertex id 1, and two edges.
     * First edge has a destination vertex 2,
     * Second edge has a destination vertex 3.
     * [1,[2,3]]
     */
	
	public class InputReaderVertexReader<V extends Writable,
										  E extends Writable,
										  M extends Writable>
		extends TextVertexReader<LongWritable, V, E, M> {
		
		public InputReaderVertexReader(
                RecordReader<LongWritable, Text> lineRecordReader) {
            super(lineRecordReader);
        }

        @Override
        public BasicVertex<LongWritable, 
        				   V, 
        				   E,
        				   M> getCurrentVertex() 
                           		throws IOException, InterruptedException {
        	BasicVertex<LongWritable, V, E, M> vertex = BspUtils.<LongWritable, V, E, M>
        									createVertex(getContext().getConfiguration());

            Text line = getRecordReader().getCurrentValue();
            
            try {
				JSONArray jsonVertex = new JSONArray(line.toString());
				LongWritable vertexId = new LongWritable(jsonVertex.getLong(0));
                
                Map<LongWritable, E> edges = Maps.newHashMap();
                JSONArray jsonEdgeArray = jsonVertex.getJSONArray(1);
                for (int i = 0; i < jsonEdgeArray.length(); ++i) {
                    edges.put(new LongWritable(jsonEdgeArray.getLong(i)), null);
                }
                vertex.initialize(vertexId, null, edges, null);
			} catch (JSONException e) {
				throw new IllegalArgumentException(
	                    "next: Couldn't get vertex from line " + line, e);
			}
			
			return vertex;            
        }
        
		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}
	
	}	
}
