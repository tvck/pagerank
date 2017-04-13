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

	public static void main(String[] args) throws IOException {
		PrintWriter writer = new PrintWriter("./Residual/NaiveAvgResidualError.txt", "UTF-8");
		int numRepititions = 5;
		double residual = 0;
		
		/* the size of the graph is hard coded into the code. */
		long size = 685230;
		
		
		for(int i = 0; i < numRepititions; i++) {
			Job job;

			job = getNaiveJob(size);


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
			residual = (double)counters.findCounter(CounterType.RESIDUAL_VALUE).getValue() / 100000;
			writer.println("Iteration " + i + " avg error " + residual / size);
			writer.println("The stage " + i + "size is: " + size);

		}
		writer.close();
	}
	public static Job getStandardJob(String s) throws IOException {
		Configuration conf = new Configuration();
		conf.set("size", s);

		Job job = new Job(conf);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Node.class);

		job.setInputFormatClass(NodeInputFormat.class);
		job.setOutputFormatClass(NodeOutputFormat.class);

		job.setJarByClass(PageRank.class);

		return job;
	}

	public static Job getNaiveJob(long s) throws IOException{

		Job job = getStandardJob("" + s);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NodeOrDouble.class);

		job.setMapperClass(NaiveMapper.class);
		job.setReducerClass(NaiveReducer.class);

		return job;
	}

	
}





