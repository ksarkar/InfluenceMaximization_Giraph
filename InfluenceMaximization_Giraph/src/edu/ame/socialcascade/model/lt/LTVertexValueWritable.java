package edu.ame.socialcascade.model.lt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;


public class LTVertexValueWritable implements Writable {
	
	private BooleanWritable isActive;	
	private DoubleWritable threshold;
	
	public LTVertexValueWritable() {
		super();
	}

	public LTVertexValueWritable(BooleanWritable isActive,
								 DoubleWritable threshold) {
		super();
		this.threshold = threshold;
		this.isActive = isActive;
	}

	public DoubleWritable getThreshold() {
		return threshold;
	}

	public void setThreshold(DoubleWritable threshold) {
		this.threshold = threshold;
	}

	public BooleanWritable getIsActive() {
		return isActive;
	}

	public void setIsActive(BooleanWritable isActive) {
		this.isActive = isActive;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.threshold = new DoubleWritable();
		this.threshold.readFields(in);
		
		this.isActive = new BooleanWritable();
		this.isActive.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.threshold.write(out);
		this.isActive.write(out);
		
	}
	
}
