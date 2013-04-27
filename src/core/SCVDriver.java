package core;

import infrastructure.GenericValue;
import infrastructure.VertexValue;
import io.VertexAdjacencyListFormat;

import java.io.IOException;

import mapreduce.ForwardPropagationMapper;
import mapreduce.ForwardPropagationReducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Driver class for the iterative map reduce social capital value calculating
 * job.
 * 
 * @author dhruvsharma1
 * 
 */
public class SCVDriver {

	public static void main(String[] args) throws IOException {
		/*
		 * Arguments list : 1. path of the input file on hdfs. 2. output
		 * directory on hdfs. 3. number of vertices. (vertex id's should be from
		 * 1 to number_of_vertices) 4. maximum iteration threshold.
		 */
		if (args.length != 4) {
			System.exit(-1);
		}
		/**
		 * If the above test is passed then the number of input arguments is
		 * correct and the iterative map reduce job can start.
		 */
		String inputPath = args[0];
		String outputPath = args[1];
		String intermediateOutputPath = "intermidiate-output/";
		int numberOfVertices = Integer.parseInt(args[2]);
		int hopsThreshold = Integer.parseInt(args[3]);

		/**
		 * Iterating over each source vertex making it the source for the
		 * current iteration.
		 */
		for (int sourceId = 1; sourceId <= numberOfVertices; sourceId++) {
			Job iterativeJob = new Job();
			int iteration = 0;
			/**
			 * looping for each hop.
			 */
			while (iteration <= hopsThreshold) {
				iterativeJob.setJarByClass(SCVDriver.class);
				/**
				 * setting the input path which points to the path of the
				 * hypergraph file path on the HDFS.
				 **/
				FileInputFormat.addInputPath(iterativeJob, new Path(inputPath));
				/**
				 * setting the output path where the intermidiate output will be
				 * written
				 * 
				 */
				FileOutputFormat.setOutputPath(iterativeJob, new Path(
						intermediateOutputPath));
				/**
				 * Setting the source id and the iteration number in the job
				 * configuration.
				 */
				iterativeJob.getConfiguration().setInt(
						CommonConstants.SOURCE_VERTEX_ID, sourceId);
				/******************************************************************/
				/* SETTING MAPPER CONFIGURATINOS */
				/******************************************************************/

				/**
				 * Setting mapper class.
				 */
				iterativeJob.setMapperClass(ForwardPropagationMapper.class);
				/**
				 * Setting input and output format classes.
				 */
				iterativeJob
						.setInputFormatClass(VertexAdjacencyListFormat.class);
				iterativeJob.setOutputFormatClass(TextOutputFormat.class);
				/**
				 * Setting mapper output key and value classes.
				 */
				iterativeJob.setMapOutputKeyClass(IntWritable.class);
				iterativeJob.setMapOutputValueClass(GenericValue.class);

				/******************************************************************/
				/* SETTING REDUCER CONFIGURATINOS */
				/******************************************************************/
				iterativeJob.setReducerClass(ForwardPropagationReducer.class);
				iterativeJob.setOutputFormatClass(TextOutputFormat.class);
				iterativeJob.setOutputKeyClass(IntWritable.class);
				iterativeJob.setOutputValueClass(VertexValue.class);
			}

		}
	}
}
