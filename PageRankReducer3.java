package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer3 extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
    
    @Override
    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {
       
    	for (Text val : values)
    	{
    		context.write(val, key);
    	}
        
        
    }

}
