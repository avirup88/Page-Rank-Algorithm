package PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper3 extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
	    Configuration conf = context.getConfiguration();
	 	    
	    Integer NODES = Integer.parseInt(conf.get("Num_Nodes"));
	    
        String page = value.toString().split("\t")[0];
        float pageRank = Float.parseFloat(value.toString().split("\t")[1]);
        
        //Filter Rows for PageRank Values >= 5/N 
        
        if (pageRank >= ((float) 5)/NODES)
        {
           context.write(new DoubleWritable(pageRank), new Text(page));
        }
        
    }
       
}
