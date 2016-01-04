package hadoop.stack.popular;

import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<TextPair, PostWritable>{

	@Override
	public int getPartition(TextPair key, PostWritable val,
			int numPart) {
		
		String toHash = key.first.toString();
		int result = 121;
//		int max = Math.max(10, toHash.length());
		int max = toHash.length();
		for (int i = 0; i < max; i++) {
		    result = (result*31 + toHash.charAt(i)) % numPart;
		}
		return result;
//		return (key.first.hashCode()& Integer.MAX_VALUE) % numPart;
	
	}
	
	

}
