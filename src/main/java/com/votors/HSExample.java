package com.votors;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HSExample {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
        private static Boolean hasSleep = false;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
            String sleep = context.getConfiguration().get("test.map.sleep");
            if (!hasSleep && sleep != null && sleep.length()>0) {
                Thread.sleep(1000 * Integer.parseInt(sleep));
                hasSleep = true;
            }
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
        private static Boolean hasSleep = false;

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
            String sleep = context.getConfiguration().get("test.reduce.sleep");
            if (!hasSleep && sleep != null && sleep.length()>0) {
                Thread.sleep(1000 * Integer.parseInt(sleep));
                hasSleep = true;
            }
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
        if (args.length < 9) {
            System.out.println("Usage: args: <input-path> <output-path> <map-mem> <reduce-mem> <map number>" +
                    " <reduce number> <map sleep> <reduce-sleep> <queue>");
            return ;
        }
		Configuration conf = new Configuration();
        conf.set("mapreduce.map.memory.mb", args[2]);
        conf.set("mapreduce.reduce.memory.mb", args[3]);
        conf.set("test.map.sleep", args[6]);
        conf.set("test.reduce.sleep", args[7]);
        conf.set("mapreduce.job.maps",args[4]);
        conf.set("mapreduce.job.reduces",args[5]);
        conf.set("mapred.job.queue.name",args[8]);

		Job job = new Job(conf, "wordcount");
		job.setJarByClass(HSExample.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		//System.out.println("World counter ");

	}

}