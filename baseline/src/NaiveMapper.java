import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;


public class NaiveMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrDouble> {
    public void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException {

        	
    	double p = value.getPageRank() / value.outgoingSize();
    	NodeOrDouble pagerankShare = new NodeOrDouble(p);
    	
    	//pass along graph structure
    	context.write(key, new NodeOrDouble(value));
    	
    	Iterator itr = value.iterator();
    	
    	while (itr.hasNext()) {
    		IntWritable nid = new IntWritable((int)itr.next());
    		context.write(nid, pagerankShare);
    	}
    	
    	
    	
	
    }
}
