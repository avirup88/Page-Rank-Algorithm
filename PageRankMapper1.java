package PageRank;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PageRankMapper1 extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
    	   
    	
            String[] Title_Links = value.toString().split("\t");
            String Title = value.toString().split("\t")[0];
            
            Integer LinkCount = Title_Links.length;

            
            for (int i=1;i<LinkCount;i++)
	    	{   
            	context.write(new Text(Title), new Text(Title_Links[i]));
	    	}
            
    }
    
}
