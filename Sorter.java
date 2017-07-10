package PageRank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Sorter extends WritableComparator {

	protected Sorter() {
		super(LongWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		LongWritable key1 = (LongWritable) a;
		LongWritable key2 = (LongWritable) b;

		// Sorting in descending order
		
		int result = key1.get() < key2.get() ? 1 : key1.get() == key2.get() ? 0 : -1;
		return result;
	}
}
