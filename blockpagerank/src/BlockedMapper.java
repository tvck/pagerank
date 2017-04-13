import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class BlockedMapper extends Mapper<IntWritable, Node, IntWritable, NodeOrBoundaryCondition> {

	public void map(IntWritable key, Node value, Context context) throws IOException, InterruptedException{
		
		/* Transfer the current node to the corresponding reducer with regard to blockID. */
		IntWritable blockID = new IntWritable(value.getBlockID());
		context.write(blockID, new NodeOrBoundaryCondition(value));
		
		System.out.println("mapper added:" + value.nodeid + " at block " + value.blockID);
		
		/* Calculate the page rank the node distributes to every outgoing edge. */
		double pagerankDistribution = value.getPageRank() / value.outgoingSize();
		
		/* We want to also send the boundary condition to the block, which contains the
		 * boundary edge and the page rank value on that edge.
		 */
		Iterator itr = value.iterator();
		while (itr.hasNext()) {
			int endNodeID = (int)itr.next();
			if (value.getBlockID() != BlockPartition.getBlockID(endNodeID)) {
				BoundaryCondition boundary 
				= new BoundaryCondition(value.nodeid, endNodeID, pagerankDistribution);
				
				IntWritable endNodeBlockID = new IntWritable(BlockPartition.getBlockID(endNodeID));
				context.write(endNodeBlockID, new NodeOrBoundaryCondition(boundary));
			}
		}

	}
}
