import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


public class PageRank {
	static final double EPSILON = 0.001; 

	public static void main(String[] args) throws IOException {
		PrintWriter writer = new PrintWriter("./Residual/NaiveAvgResidualError.txt", "UTF-8");
		int numRepititions = 6;
		double globalResidual = 0.0;
		double avgNumOfBlockIteration = 0.0;
		 
		boolean shouldTerminate = false;
			
		
		for(int i = 0; i < numRepititions; i++) {
			Job job;

			job = getBlockedJob();


			String inputPath = i == 0 ? "input" : "stage" + (i-1);
			String outputPath = "stage" + i;

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));

			try { 
				job.waitForCompletion(true);
			} catch(Exception e) {
				System.err.println("ERROR IN JOB: " + e);
				return;
			}

			// Set up residual and size
			Counters counters = job.getCounters();
			globalResidual = (double)counters.findCounter(CounterType.RESIDUAL_VALUE).getValue() / 100000;
			
			avgNumOfBlockIteration 
			= (double)counters.findCounter(CounterType.NUM_OF_ITERATION).getValue() / BlockPartition.getNumOfBlocks();
			
			shouldTerminate = globalResidual < (BlockPartition.getNumOfBlocks() * EPSILON) ? true : false;
			writer.println("The stage " + i + " avg error: " + globalResidual / BlockPartition.getNumOfBlocks());
			//writer.println("The stage " + i + " size is: " + BlockPartition.getGraphSize());
			writer.println("The stage " + i + " ave number of iterations is: " + avgNumOfBlockIteration);
			writer.println("The stage " + i + " converge?: " + shouldTerminate);
			writer.println("====================================================================");
		}
		writer.close();
	}
	public static Job getStandardJob() throws IOException {
		Configuration conf = new Configuration();

		Job job = new Job(conf);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Node.class);

		job.setInputFormatClass(NodeInputFormat.class);
		job.setOutputFormatClass(NodeOutputFormat.class);

		job.setJarByClass(PageRank.class);

		return job;
	}

	public static Job getBlockedJob() throws IOException{

		Job job = getStandardJob();

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NodeOrBoundaryCondition.class);

		job.setMapperClass(BlockedMapper.class);
		job.setReducerClass(BlockedReducer.class);

		return job;
	}

	
}





