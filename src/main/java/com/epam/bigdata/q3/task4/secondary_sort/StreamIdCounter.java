package com.epam.bigdata.q3.task4.secondary_sort;

public enum StreamIdCounter {
	
	NULL(0), UNIT(1);
	
	private final int streamId;
	
	private StreamIdCounter(int streamId) {
		this.streamId = streamId;
	}

	public int getStreamId() {
		return streamId;
	}
	
}
