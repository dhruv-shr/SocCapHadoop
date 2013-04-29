package core;

import infrastructure.GenericValue;
import infrastructure.VertexValue;
import io.VertexAdjacencyListFormat;
import mapreduce.ForwardPropagationMapper;
import mapreduce.ForwardPropagationReducer;
import mapreduce.ResetMapper;
import mapreduce.ResetReducer;
import mapreduce.ReversePropagationMapper;
import mapreduce.ReversePropagationReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CompleteDriver {

	public static void main(String[] args) throws Exception {
		/*
		 * Arguments list : 1. path of the input file on hdfs. 2. output
		 * directory on hdfs. 3. number of vertices. (vertex id's should be from
		 * 1 to number_of_vertices) 4. maximum iteration threshold.
		 */
		if (args.length != 3) {
			System.exit(-1);
		}
		/**
		 * If the above test is passed then the number of input arguments is
		 * correct and the iterative map reduce job can start.
		 */
		String path0 = args[0];

		String intermediateOutputPath = "intermidiate-output/";
		int numberOfVertices = Integer.parseInt(args[1]);
		int hopsThreshold = Integer.parseInt(args[2]);
		String inputPath = "";
		String outputPath = "";

		/**
		 * Iterating over each source vertex making it the source for the
		 * current iteration.
		 */
		// for (int sourceId = 1; sourceId <= numberOfVertices; sourceId++) {

		for (int num = 1; num <= numberOfVertices; num++) {

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
				 * setting the input path which points to the path of the
				 * hypergraph file path on the HDFS.
				 **/

				Job iterativeJob = new Job(conf, "Social Capital");
				iterativeJob.setJarByClass(SCVDriver.class);
				
			if(num==1)	
				inputPath = path0 + "" + iteration;
			else
				inputPath = path0;
			
				outputPath = path0 + "" + (iteration + 1);
				FileInputFormat.addInputPath(iterativeJob, new Path(inputPath));
				/**
				 * setting the output path where the intermidiate output will be
				 * written
				 * 
				 */
				FileOutputFormat.setOutputPath(iterativeJob, new Path(
						outputPath));
				/**
				 * Setting the source id and the iteration number in the job
				 * configuration.
				 */
				iterativeJob.getConfiguration().setInt(
						CommonConstants.SOURCE_VERTEX_ID, num);
				iterativeJob.getConfiguration().setInt(
						CommonConstants.CURRENT_ITERATION, iteration);
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

				iterativeJob.waitForCompletion(true);
				iteration++;
			}

			// }

			// Mapping Phase is Over
			// Outpath of Mapper would be at ./social<Iteration>

			// Input of Reducer would be ./social<Iteartion>
			// Iteration Again Set to 0
			iteration = 0;
			/**
			 * looping for each hop.
			 */
			// Setting it once so that the output of Mapper becomes the input of
			// Reducer.
			path0 = outputPath;

			// Condition Not Equality as iteration in reducers would be one less
			// than Mapper.
			while (iteration < hopsThreshold) {

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
				 * setting the input path which points to the path of the
				 * hypergraph file path on the HDFS.
				 **/

				Job iterativeJob = new Job(conf,
						"Social Capital Back Propogation");
				iterativeJob.setJarByClass(BackPropogationDriver.class);

				// First time the outputPath would be that of the outputPath of
				// the previous Mapping Phase

				if (iteration == 0)
					inputPath = path0;
				else
					inputPath = path0 + "" + iteration;

				outputPath = path0 + "" + (iteration + 1);
				FileInputFormat.addInputPath(iterativeJob, new Path(inputPath));
				/**
				 * setting the output path where the intermidiate output will be
				 * written
				 * 
				 */
				FileOutputFormat.setOutputPath(iterativeJob, new Path(
						outputPath));
				/**
				 * Setting the source id and the iteration number in the job
				 * configuration.
				 */
				iterativeJob.getConfiguration().setInt(
						CommonConstants.SOURCE_VERTEX_ID, num);
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

			// Reseting the output Once
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
			inputPath = outputPath;
			outputPath = (inputPath + "1");
			path0 = outputPath;
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
			/******************************************************************/
			/* SETTING MAPPER CONFIGURATINOS */
			/******************************************************************/

			/**
			 * Setting mapper class.
			 */
			iterativeJob.setMapperClass(ResetMapper.class);
			iterativeJob.setInputFormatClass(TextInputFormat.class);
			iterativeJob.setMapOutputKeyClass(Text.class);
			iterativeJob.setMapOutputValueClass(Text.class);

			/******************************************************************/
			/* SETTING REDUCER CONFIGURATINOS */
			/******************************************************************/
			iterativeJob.setReducerClass(ResetReducer.class);
			iterativeJob.setOutputFormatClass(TextOutputFormat.class);
			iterativeJob.setOutputKeyClass(Text.class);
			iterativeJob.setOutputValueClass(Text.class);

			iterativeJob.waitForCompletion(true);
		}
	}

}
