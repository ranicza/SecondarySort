package com.epam.bigdata.q3.task4.secondary_sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class CompKeyPartitioner extends Partitioner<CompositeKey, NullWritable> {

	/*
	 * It's doesn't need to carry any information in the value, because we can
	 * get the first (maximum) timestamp in the Reducer from the key, so it's
	 * used a NullWritable.
	 * 
	 */
	@Override
	public int getPartition(CompositeKey key, NullWritable arg, int numPartitions) {
		return key.getiPinyouId().hashCode() % numPartitions;
	}

}
