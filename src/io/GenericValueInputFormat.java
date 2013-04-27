package io;

import java.io.IOException;

import infrastructure.GenericValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class GenericValueInputFormat extends
		FileInputFormat<IntWritable, GenericValue> {

	@Override
	public RecordReader<IntWritable, GenericValue> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		context.setStatus(inputSplit.toString());
		return new GenericValueReader();
	}

}
