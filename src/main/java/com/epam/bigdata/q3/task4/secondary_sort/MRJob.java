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


public class MRJob {
	
	public static final String USAGE_ERROR = "Usage error: <in> <out>";
	public static final String JOB_NAME = "MRJOb";
	
	public static class Map extends Mapper<LongWritable, Text, CompositeKey, Text> {
		private Text content = new Text();
		private CompositeKey comKey = new CompositeKey();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();			
			String[] tokens = line.split("\\s+");
			content.set(line);
			
			System.out.println("iPinyouId: " + tokens[2]);
			System.out.println("timestamp: " + tokens[1]);
			
			comKey.setiPinyouId(tokens[1]);
			comKey.setTimestamp(Long.parseLong(tokens[2]));
			
			context.write(comKey, content);
		}		
	}
	
	public static class Reduce extends Reducer<CompositeKey, Text, NullWritable, Text> {

		public void reduce(CompositeKey key, Iterable<Text> values, Context context) {	
			String line = null;
			int streamId = 0;
			int siteImpressionSum = 0;
			
			try {						
				for (Text val : values) {
					context.write(NullWritable.get(), val);	
					
					line = val.toString();
					streamId = Integer.parseInt(line.substring(line.length()-1));
					if (streamId == 1) {
						siteImpressionSum++;
					}
				}			

			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
		if (otherArgs.length < 2) {
			System.err.println(USAGE_ERROR);
			System.exit(2);
		}

		// Create a new job
		Job job = new Job(conf, JOB_NAME);
		job.setJarByClass(MRJob.class);
		
		//Sets mapper & reduce
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		//for secondary sort
		job.setPartitionerClass(CompKeyPartitioner.class);
		job.setSortComparatorClass(CompKeyComparator.class);
		job.setGroupingComparatorClass(CompKeyGroupComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		boolean status = job.waitForCompletion(true);
		
		System.exit(status ? 0 : 1);
	}
	
}