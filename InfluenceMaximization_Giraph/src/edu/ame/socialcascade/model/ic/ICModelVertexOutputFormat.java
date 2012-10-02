package edu.ame.socialcascade.model.ic;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.giraph.lib.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

public class ICModelVertexOutputFormat extends
					TextVertexOutputFormat<LongWritable, 
										   ICVertexValueWritable,
										   DoubleWritable> {

	@Override
	public VertexWriter<LongWritable, ICVertexValueWritable, DoubleWritable>
				createVertexWriter(TaskAttemptContext context)
							throws IOException, InterruptedException {
		RecordWriter<Text, Text> recordWriter = textOutputFormat.getRecordWriter(context);
		return new ICModelVertexWriter(recordWriter);
	}
	
	/**
	* VertexWriter that supports CascadeVertexVertex
	*/
	public class ICModelVertexWriter extends
							TextVertexWriter<LongWritable, 
											 ICVertexValueWritable, 
											 DoubleWritable> {
		public ICModelVertexWriter(RecordWriter<Text, Text> lineRecordWriter) {
			super(lineRecordWriter);
		}
		
		@Override
		public void writeVertex(BasicVertex<LongWritable, 
											ICVertexValueWritable,
											DoubleWritable, 
											?> vertex) throws IOException, InterruptedException {
			
			JSONArray jsonVertex = new JSONArray();
			try {
				jsonVertex.put(vertex.getVertexId().get());
				
				JSONArray jsonVertexValueArray = new JSONArray();
				ICVertexValueWritable vertexVal = vertex.getVertexValue();
				jsonVertexValueArray.put(vertexVal.getIsActive().get());
				JSONArray jsonIsContagious = new JSONArray();
				jsonIsContagious.put(vertexVal.getIsContagious().get());
				jsonVertexValueArray.put(jsonIsContagious);
				
				jsonVertex.put(jsonVertexValueArray);
				
				JSONArray jsonEdgeArray = new JSONArray();
				Iterator<LongWritable> outEdges = vertex.getOutEdgesIterator();
				while (outEdges.hasNext()) {
					LongWritable targetVertexId = outEdges.next();
					JSONArray jsonEdge = new JSONArray();
					jsonEdge.put(targetVertexId.get());
					jsonEdge.put(vertex.getEdgeValue(targetVertexId).get());
					jsonEdgeArray.put(jsonEdge);
				}
				jsonVertex.put(jsonEdgeArray);
			} catch (JSONException e) {
				throw new IllegalArgumentException(
						"writeVertex: Couldn't write vertex " + vertex);
			}
			getRecordWriter().write(new Text(jsonVertex.toString()), null);
		}
	}

}
