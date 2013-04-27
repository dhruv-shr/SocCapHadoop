package io;

import java.io.IOException;

import infrastructure.VertexValue;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class VertexAdjacencyListFormat extends
		FileInputFormat<IntWritable, VertexValue> {

	@Override
	public RecordReader<IntWritable, VertexValue> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		context.setStatus(inputSplit.toString());
		return new VertexAdjacencyListReader();
	}

}
