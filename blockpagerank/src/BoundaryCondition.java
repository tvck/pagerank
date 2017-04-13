import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class BoundaryCondition implements Writable {
	int fromNodeID;
	int toNodeID;
	double pageRank;

	// Here for internal Hadoop purposes only. Don's use this constructor!
	public BoundaryCondition() {
		fromNodeID = -1;
		toNodeID = -1;
		pageRank = -1.0; 
	}
	
	public BoundaryCondition(int from) {
		fromNodeID = from;
		toNodeID = -1;
		pageRank = -1.0;
	}

	public BoundaryCondition(int from, int to, double pr) {
		fromNodeID = from;
		toNodeID = to;
		pageRank = pr;
	}

	// Get the fromNodeID
	public int getFromNodeID() {
		return fromNodeID;
	}

	// Set the from Node ID
	public void setFromNodeID(int from) {
		fromNodeID = from;
	}

	// Get the toNodeID
	public int getToNodeID() {
		return toNodeID;
	}

	// Seet the toNodeID
	public void setToNodeID(int to) {
		toNodeID = to;
	}

	//Get the PageRank of this node.
	public double getPageRank() {
		return pageRank;
	}

	//Set the PageRank of this node
	public void setPageRank(double pr) {
		pageRank = pr;
	}


	//Used for internal Hadoop purposes.
	//Describes how to write this node across a network
	public void write(DataOutput out) throws IOException {
		out.writeInt(fromNodeID);
		out.writeInt(toNodeID);
		out.writeDouble(pageRank);
	}

	//Used for internal Hadoop purposes
	//Describes how to read this node from across a network
	public void readFields(DataInput in) throws IOException {
		fromNodeID = in.readInt();
		toNodeID = in.readInt();
		pageRank = in.readDouble();
	}

}
