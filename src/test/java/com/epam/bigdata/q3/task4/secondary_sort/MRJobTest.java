package com.epam.bigdata.q3.task4.secondary_sort;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class MRJobTest {
	MapDriver<LongWritable, Text, CompositeKey, Text> mapDriver;
	ReduceDriver<CompositeKey, Text, NullWritable, Text> reduceDriver;
	MapReduceDriver<LongWritable, Text, CompositeKey, Text, NullWritable, Text> mapReduceDriver;

	private String input1 = "c49b9354a84ce183eb4697df07298d46	20130606001907469	VhkSLxSELTuOkGn	Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729)	27.36.3.*	216	222	3	tMxYQ19aM98	a0ab67bbe87f2e024338b093856580	null	LV_1001_LDVi_LD_ADX_1	300	250	0	0	100	00fccc64a1ee2809348509b7ac2a97a5	241	3427	282825712806	0";
	private String input2 = "2a72c8727a42de2ceaaaf8b17d7654d5	20130606001907462	VhkSLxSELTuOkGn	Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729)	27.36.3.*	216	222	3	tMxYQ19aM98	a0ab67bbe87f2e024338b093856580	null	LV_1001_LDVi_LD_ADX_2	300	250	0	0	100	00fccc64a1ee2809348509b7ac2a97a5	241	3427	282163094182	0";
	private String input3 = "75fb0b4a58fa9ca2297206327d28c18	20130606001907465	VhLrLu5yOlFstGR	Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; InfoPath.2; .NET CLR 2.0.50727)	110.90.50.*	124	125	3	5F1RQS9rg5scFsf	1389f47e7cc43a04020e5744dbfd31b8	null	ALLINONE_F_Width1	1000	90	0	0	70	d01411218c7c86a893d4d4f68810a0416	300	3386	282825712806	1";

	private String iPinyouId1 = "VhkSLxSELTuOkGn";
	private long timestamp1 = 20130606001907469L;
	private long timestamp2 = 20130606001907462L;

	private String iPinyouId2 = "VhLrLu5yOlFstGR";
	private long timestamp3 = 20130606001907465L;

	@Before
	public void setUp() {
		MRJob.Map mapper = new MRJob.Map();
		MRJob.Reduce reducer = new MRJob.Reduce();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text(input1));
		mapDriver.withInput(new LongWritable(), new Text(input2));
		mapDriver.withInput(new LongWritable(), new Text(input3));
		mapDriver.withOutput(new CompositeKey(iPinyouId1, timestamp1), new Text(input1));
		mapDriver.withOutput(new CompositeKey(iPinyouId1, timestamp2), new Text(input2));
		mapDriver.withOutput(new CompositeKey(iPinyouId2, timestamp3), new Text(input3));

		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<Text> values1 = new ArrayList<Text>();
		values1.add(new Text(input1));

		List<Text> values2 = new ArrayList<Text>();
		values2.add(new Text(input2));

		List<Text> values3 = new ArrayList<Text>();
		values3.add(new Text(input3));

		reduceDriver.withInput(new CompositeKey(iPinyouId1, timestamp1), values1);
		reduceDriver.withInput(new CompositeKey(iPinyouId1, timestamp2), values2);
		reduceDriver.withInput(new CompositeKey(iPinyouId2, timestamp3), values3);

		reduceDriver.withOutput(NullWritable.get(), new Text(input1));
		reduceDriver.withOutput(NullWritable.get(), new Text(input2));
		reduceDriver.withOutput(NullWritable.get(), new Text(input3));

		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce() throws IOException {
		mapReduceDriver.withInput(new LongWritable(), new Text(input1));
		mapReduceDriver.withInput(new LongWritable(), new Text(input2));
		mapReduceDriver.withInput(new LongWritable(), new Text(input3));

		mapReduceDriver.withOutput(NullWritable.get(), new Text(input2));
		mapReduceDriver.withOutput(NullWritable.get(), new Text(input1));
		mapReduceDriver.withOutput(NullWritable.get(), new Text(input3));

		mapReduceDriver.runTest();
	}
}
