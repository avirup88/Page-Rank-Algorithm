package PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer1 extends Reducer<Text, Text, Text, Text> {


	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


	    Configuration conf = context.getConfiguration();
	 	    
	    Integer NODES = Integer.parseInt(conf.get("Num_Nodes"));
		
		//Initial PageRank Value
		
		
		String links = ((float) 1 / NODES) + "\t";
		

		for (Text value : values) {
			
			links += value.toString();
			links += "~";
		}
	

		
			context.write(key, new Text(links));
		
	}

}
