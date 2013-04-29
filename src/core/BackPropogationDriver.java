package core;

import infrastructure.GenericValue;
import infrastructure.VertexValue;
import io.VertexAdjacencyListFormat;

import java.io.IOException;

import mapreduce.ForwardPropagationMapper;
import mapreduce.ReversePropagationReducer;
import mapreduce.ReversePropagationMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Driver class for the iterative map reduce social capital value calculating
 * job.
 * 
 * @author dhruvsharma1
 * 
 */
public class BackPropogationDriver {

	public static void main(String[] args) throws Exception {
		/*
		 * Arguments list : 1. path of the input file on hdfs. 2. output
		 * directory on hdfs. 3. number of vertices. (vertex id's should be from
		 * 1 to number_of_vertices) 4. maximum iteration threshold.
		 */
		
		/**
		 * If the above test is passed then the number of input arguments is
		 * correct and the iterative map reduce job can start.
		 */
		String path0 = args[0];

		String intermediateOutputPath = "intermidiate-output/";
		int numberOfVertices = Integer.parseInt(args[1]);
		int hopsThreshold = Integer.parseInt(args[2]);

		/**
		 * Iterating over each source vertex making it the source for the
		 * current iteration.
		 */
		// for (int sourceId = 1; sourceId <= numberOfVertices; sourceId++) {

		int iteration = 0;
		/**
		 * looping for each hop.
		 */
		while (iteration <= hopsThreshold) {

			Configuration conf = new Configuration();
			conf.set("mapred.job.reduce.input.buffer.percent", "0.7");
			conf.set("mapred.inmem.merge.threshold", "0");
			conf.set("io.sort.factor", "95");
			conf.set("io.sort.mb", "500");
			conf.set("mapred.reduce.parallel.copies", "30");
			conf.set("mapred.map.tasks", "5");
			// conf.set("mapred.tasktracker.map.tasks.maximum","6");
			conf.set("mapred.reduce.tasks", "" + 4);
			conf.set("mapred.compress.map.output", "true");
			conf.set("mapred.output.compression.type", "BLOCK");
			conf.set("mapred.map.output.compression.codec",
					"org.apache.hadoop.io.compress.GzipCodec");
			/**
			 * setting the input path which points to the path of the hypergraph
			 * file path on the HDFS.
			 **/

			Job iterativeJob = new Job(conf, "Social Capital Back Propogation");
			iterativeJob.setJarByClass(BackPropogationDriver.class);
			String inputPath = path0 + "" + iteration;
			String outputPath = path0 + "" + (iteration + 1);
			FileInputFormat.addInputPath(iterativeJob, new Path(inputPath));
			/**
			 * setting the output path where the intermidiate output will be
			 * written
			 * 
			 */
			FileOutputFormat.setOutputPath(iterativeJob, new Path(outputPath));
			/**
			 * Setting the source id and the iteration number in the job
			 * configuration.
			 */
			iterativeJob.getConfiguration().setInt(
					CommonConstants.SOURCE_VERTEX_ID, 1);
			iterativeJob.getConfiguration().setInt(
					CommonConstants.CURRENT_ITERATION, iteration);
			/******************************************************************/
			/* SETTING MAPPER CONFIGURATINOS */
			/******************************************************************/

			/**
			 * Setting mapper class.
			 */
			iterativeJob.setMapperClass(ReversePropagationMapper.class);
			iterativeJob.setInputFormatClass(TextInputFormat.class);
			iterativeJob.setMapOutputKeyClass(Text.class);
			iterativeJob.setMapOutputValueClass(Text.class);

			/******************************************************************/
			/* SETTING REDUCER CONFIGURATINOS */
			/******************************************************************/
			iterativeJob.setReducerClass(ReversePropagationReducer.class);
			iterativeJob.setOutputFormatClass(TextOutputFormat.class);
			iterativeJob.setOutputKeyClass(Text.class);
			iterativeJob.setOutputValueClass(Text.class);

			iterativeJob.waitForCompletion(true);
			iteration++;
		}

		// }
	}
}
