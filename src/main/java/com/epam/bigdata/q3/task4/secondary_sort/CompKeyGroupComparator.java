package com.epam.bigdata.q3.task4.secondary_sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompKeyGroupComparator extends WritableComparator {

	public CompKeyGroupComparator() {
		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CompositeKey key1 = (CompositeKey) a;
		CompositeKey key2 = (CompositeKey) b;

		return key1.getiPinyouId().compareToIgnoreCase(key2.getiPinyouId());
	}
}
