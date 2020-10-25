package com.mapreduce.mappers;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Logger logger = Logger.getLogger(WordCountMapper.class.getName());

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		logger.log(Level.INFO, "WordCountMapper.mapper input : {} - {}", new Object[] { key, value });
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line, " ");
		while (st.hasMoreTokens()) {
			word.set(st.nextToken());
			context.write(word, one);
			logger.log(Level.INFO, "WordCountMapper.mapper context : {} - {}", new Object[] { word, one });
		}
	}
}
