import java.util.Arrays;


public class BlockPartition {
	/* The the number of nodes in the graph. */
	private static final int GRAPH_SIZE = 685230;

	/* The partitioned array for index binary search of the blocks. */
	private static final int blockPartition[] = {10327, 20372, 30628, 40644, 50461, 60840, 70590, 80117, 90496, 100500, 
		110566, 120944, 130998, 140573, 150952, 161331, 171153, 181513, 191624, 202003, 
		212382, 222761, 232592, 242877, 252937, 263148, 273209, 283472, 293254, 303042, 
		313369, 323521, 333882, 343662, 353644, 363928, 374235, 384553, 394928, 404711, 
		414616, 424746, 434706, 444488, 454284, 464397, 474195, 484049, 493967, 503751, 
		514130, 524509, 534708, 545087, 555466, 565845, 576224, 586603, 596584, 606366, 
		616147, 626447, 636239, 646021, 655803, 665665, 675447, 685229};
	
	
	/** 
	 * return the corresponding blockID of given nodeID.
	 * @param nodeID
	 * @return
	 */
	public static int getBlockID(int nodeID) {
		int index = Arrays.binarySearch(blockPartition, nodeID);
		if (index < 0) {
			index = -(index + 1);
		}
		return index;
	}
	
	/**
	 * return the graph size measured by total number of nodes.
	 * @return
	 */
	public static int getGraphSize() {
		return GRAPH_SIZE;
	}

	public static int getNumOfBlocks() {
		return blockPartition.length;
	}




}
