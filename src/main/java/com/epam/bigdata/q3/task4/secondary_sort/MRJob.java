package com.epam.bigdata.q3.task4.secondary_sort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;

public class MRJob {

	public static final String DELIMETER = "\\s+";
	public static final String USAGE_ERROR = "Usage error: <in> <out>";
	public static final String JOB_NAME = "MRJOb";
	public static final String NULL = "null";

	public static class Map extends Mapper<LongWritable, Text, CompositeKey, Text> {
		private Text content = new Text();
		private CompositeKey comKey = new CompositeKey();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] tokens = line.split(DELIMETER);
			content.set(line);

			comKey.setiPinyouId(tokens[2]);
			comKey.setTimestamp(Long.parseLong(tokens[1]));

			context.write(comKey, content);
		}
	}

	public static class Reduce extends Reducer<CompositeKey, Text, NullWritable, Text> {
		private long max = 0;

		public void reduce(CompositeKey key, Iterable<Text> values, Context context) {

			int streamId = 0;
			long siteImpressionSum = 0;

			try {
				for (Text val : values) {
					context.write(NullWritable.get(), val);

					String line = val.toString();
					streamId = Integer.parseInt(line.substring(line.length() - 1));
					if (streamId == StreamIdCounter.UNIT.getStreamId()) {
						siteImpressionSum++;
					}
				}

				if (siteImpressionSum > max && !key.getiPinyouId().equalsIgnoreCase(NULL)) {
					max = siteImpressionSum;

					Counter siteImpressionCounter = context.getCounter(StreamIdCounter.class.getName(),
							key.getiPinyouId());
					siteImpressionCounter.setValue(Math.max(max, siteImpressionCounter.getValue()));
				}

			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		String iPinyouId = null;
		long max = 0;

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 2) {
			System.err.println(USAGE_ERROR);
			System.exit(2);
		}

		// Create a new job
		Job job = new Job(conf, JOB_NAME);
		job.setJarByClass(MRJob.class);

		// Sets mapper & reduce
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);

		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		// for secondary sort
		job.setPartitionerClass(CompKeyPartitioner.class);
		job.setSortComparatorClass(CompKeyComparator.class);
		job.setGroupingComparatorClass(CompKeyGroupComparator.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		boolean status = job.waitForCompletion(true);

		for (Counter counter : job.getCounters().getGroup(StreamIdCounter.class.getName())) {
			if (counter.getValue() > max) {
				max = counter.getValue();
				iPinyouId = counter.getName();
			}
		}

		System.out.println("iPinyouId: " + iPinyouId + ", site-impression counter: " + max);

		System.exit(status ? 0 : 1);
	}

}
