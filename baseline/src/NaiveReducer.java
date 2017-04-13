import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class NaiveReducer extends Reducer<IntWritable, NodeOrDouble, IntWritable, Node> {
	public final static double DAMPING_FACTOR = 0.85;
	
    public void reduce(IntWritable key, Iterable<NodeOrDouble> values, Context context)
	throws IOException, InterruptedException {
    	
    	String sizeString = context.getConfiguration().get("size");
    	long size = Long.parseLong(sizeString);
    	System.out.println("naive reducer size" +size);
    	
    	// construct a noe with current node id
    	Node currentNode = null;
    	double pagerank = 0.0;
    	double residual = 0.0;
    	for (NodeOrDouble value : values) {
    		// if the value is a node, i.e. the node is the node self passed from
    		// mapper. then we could assign it's outgoing to the current node.
    		if (value.isNode()) {
    			currentNode = value.getNode();
    			
    		} else {
    			pagerank += value.getDouble();
    		}
    	}
    	
    	// Calculate the page rank by applying the damping factor
  
    	System.out.println("naive reducer size:" + size);
    	pagerank = DAMPING_FACTOR * pagerank + (1 - DAMPING_FACTOR) / size;
    	
    	
    	// Set up the residual for this node
    	residual = Math.abs(currentNode.getPageRank() - pagerank) / pagerank;
    	
    	// Using the Hadoop counter to set up the global residual
    	long magnifiedResidual = (long)(residual * 100000);
		context.getCounter(CounterType.RESIDUAL_VALUE).increment(magnifiedResidual);
		
    	currentNode.setPageRank(pagerank);
    	context.write(key, currentNode);
    	
    }
}
