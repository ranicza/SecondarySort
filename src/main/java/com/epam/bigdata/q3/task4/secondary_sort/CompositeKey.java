package com.epam.bigdata.q3.task4.secondary_sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements Writable, WritableComparable<CompositeKey>{
	
	private Long timestamp;
	private String iPinyouId;
	
	public CompositeKey() {}
	
	public CompositeKey(String iPinyouId, Long timestamp) {
		this.iPinyouId = iPinyouId;
		this.timestamp = timestamp;
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(iPinyouId);
		out.writeLong(timestamp);
		
	}
	
	public void readFields(DataInput in) throws IOException {
		iPinyouId = in.readUTF();
		timestamp = in.readLong();
		
	}
	
	public int compareTo(CompositeKey o) {
		int iPinyouIdCmp = iPinyouId.compareToIgnoreCase(o.getiPinyouId());
		if (iPinyouIdCmp != 0) {
			return iPinyouIdCmp;
		}		
		return Long.compare(timestamp, o.getTimestamp());
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getiPinyouId() {
		return iPinyouId;
	}

	public void setiPinyouId(String iPinyouId) {
		this.iPinyouId = iPinyouId;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((iPinyouId == null) ? 0 : iPinyouId.hashCode());
		result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		CompositeKey other = (CompositeKey) obj;
		if (iPinyouId == null) {
			if (other.iPinyouId != null)
				return false;
		} else if (!iPinyouId.equals(other.iPinyouId))
			return false;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (!timestamp.equals(other.timestamp))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "CompositeKey [timestamp=" + timestamp + ", iPinyouId=" + iPinyouId + "]";
	}


}
