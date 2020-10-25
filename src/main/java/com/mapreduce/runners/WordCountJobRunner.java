package com.mapreduce.runners;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mapreduce.mappers.WordCountMapper;
import com.mapreduce.reducers.WordCountReducer;

public class WordCountJobRunner {

	static Logger logger = Logger.getLogger(WordCountJobRunner.class.getName());

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		logger.log(Level.INFO, "WordCountJobRunner.main args : {}", args);
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		if ((remainingArgs.length != 2)) {
			logger.log(Level.INFO, "WordCountJobRunner.main args : {}", args);
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountJobRunner.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
		logger.log(Level.INFO, "WordCountJobRunner.main exit");
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
