package core;

import mapreduce.ReversePropagationMapper;
import mapreduce.ReversePropagationReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import mapreduce.ResetMapper;
import mapreduce.ResetReducer;

public class ResetDriver {

	public static void main(String args[]) throws Exception{
		
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
		String inputPath = args[0];
		String outputPath = (args[0]+"1");
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
