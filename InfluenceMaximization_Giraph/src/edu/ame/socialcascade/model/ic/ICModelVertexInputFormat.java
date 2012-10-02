package edu.ame.socialcascade.model.ic;

import java.io.IOException;
import java.util.Map;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.BspUtils;
import org.apache.giraph.graph.VertexReader;
import org.apache.giraph.lib.TextVertexInputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Maps;

public class ICModelVertexInputFormat extends
										TextVertexInputFormat<LongWritable,
																ICVertexValueWritable,
																DoubleWritable,
																BooleanWritable> {
	@Override
	public VertexReader<LongWritable,
						ICVertexValueWritable,
						DoubleWritable,
						BooleanWritable> createVertexReader(InputSplit split,
															TaskAttemptContext context)
																	throws IOException {
		return new ICModelVertexReader(
				textInputFormat.createRecordReader(split, context));
	}

	/**
	* VertexReader that supports CascadeVertex. In this
	* case, the edge values are not used.  The files should be in the
	* following JSON format:
	* JSONArray(<vertex id>, JSONArray(<isActive>JSONArray(<isContagious>)),
	*           JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
	* Here is an example with vertex id 1, vertex value <true, true>, and two edges.
	* First edge has a destination vertex 2, edge value 0.72234.
	* Second edge has a destination vertex 3, edge value 0.65778.
	* [1,[true,[true]],[[2,0.72234],[3,0.65778]]]
	*/
	
	public class ICModelVertexReader extends
					TextVertexReader<LongWritable, 
										ICVertexValueWritable,
										DoubleWritable,
										BooleanWritable>  {
	
		public ICModelVertexReader(RecordReader<LongWritable, Text> lineRecordReader) {
			super(lineRecordReader);
		}
		
		@Override
		public BasicVertex<LongWritable, 
							ICVertexValueWritable,
							DoubleWritable,
							BooleanWritable> getCurrentVertex() 
									throws IOException, InterruptedException {
			BasicVertex<LongWritable, 
						ICVertexValueWritable,
						DoubleWritable,
						BooleanWritable> vertex = BspUtils.<LongWritable, 
															ICVertexValueWritable,
															DoubleWritable,
															BooleanWritable> createVertex(getContext().getConfiguration());
			
			Text line = getRecordReader().getCurrentValue();
			try {
				JSONArray jsonVertex = new JSONArray(line.toString());
				LongWritable vertexId = new LongWritable(jsonVertex.getLong(0));
				
				JSONArray jsonVertexValueArray = jsonVertex.getJSONArray(1);
				ICVertexValueWritable vertexValue = new ICVertexValueWritable(
											new BooleanWritable(jsonVertexValueArray.getBoolean(0)),
											new BooleanWritable(jsonVertexValueArray.getJSONArray(1).getBoolean(0)));
				
				Map<LongWritable, DoubleWritable> edges = Maps.newHashMap();
				JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
				for (int i = 0; i < jsonEdgeArray.length(); ++i) {
					JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
					edges.put(new LongWritable(jsonEdge.getLong(0)),
							  new DoubleWritable(jsonEdge.getDouble(1)));
				}
				vertex.initialize(vertexId, vertexValue, edges, null);
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

