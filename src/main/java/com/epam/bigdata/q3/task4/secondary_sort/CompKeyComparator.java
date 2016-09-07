package com.epam.bigdata.q3.task4.secondary_sort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompKeyComparator extends WritableComparator{
	
	public CompKeyComparator() {
		super(CompositeKey.class, true);
	}
	
	@Override
    public int compare(WritableComparable a, WritableComparable b) {
		CompositeKey key1 = (CompositeKey) a;
		CompositeKey key2 = (CompositeKey) b;
		
		int iPinyouIdCmp = key1.getiPinyouId().compareToIgnoreCase(key2.getiPinyouId());		
		if (iPinyouIdCmp != 0) {
			return iPinyouIdCmp;
		}
		
		return Long.compare(key1.getTimestamp(), key2.getTimestamp());
	}

}
