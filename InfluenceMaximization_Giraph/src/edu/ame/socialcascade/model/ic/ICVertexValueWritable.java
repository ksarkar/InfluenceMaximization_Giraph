package edu.ame.socialcascade.model.ic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Writable;

public class ICVertexValueWritable implements Writable {
	
	private BooleanWritable isActive;
	private BooleanWritable isContagious;

	public ICVertexValueWritable() {
		super();
	}

	public ICVertexValueWritable(BooleanWritable isActive,
			BooleanWritable isContagious) {
		super();
		this.isActive = isActive;
		this.isContagious = isContagious;
	}

	public BooleanWritable getIsActive() {
		return isActive;
	}

	public void setIsActive(BooleanWritable isActive) {
		this.isActive = isActive;
	}

	public BooleanWritable getIsContagious() {
		return isContagious;
	}

	public void setIsContagious(BooleanWritable isContagious) {
		this.isContagious = isContagious;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.isActive = new BooleanWritable();
		this.isActive.readFields(in);
		
		this.isContagious = new BooleanWritable();
		this.isContagious.readFields(in);		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.isActive.write(out);
		this.isContagious.write(out);
	}
	

}
