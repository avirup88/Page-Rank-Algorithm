package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer0
extends Reducer<Text,IntWritable,Integer, NullWritable> {


public void reduce(Text key, Iterable<IntWritable> values,
                Context context
                ) throws IOException, InterruptedException {
int sum=0;
	
for (IntWritable val : values) 
{
 sum = sum + val.get();

}

		context.write(sum,NullWritable.get());
		



}
}