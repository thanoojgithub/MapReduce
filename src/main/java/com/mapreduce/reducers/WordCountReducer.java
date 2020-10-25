package com.mapreduce.reducers;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	Logger logger = Logger.getLogger(WordCountReducer.class.getName());

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		logger.log(Level.INFO, "WordCountReducer.reduce input : {} - {}", new Object[] { key, values });
		int sum = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		while (valuesIt.hasNext()) {
			sum = sum + valuesIt.next().get();
		}
		context.write(key, new IntWritable(sum));
		logger.log(Level.INFO, "WordCountReducer.reduce context : {} - {}", new Object[] { key, sum });
	}
}
