package PageRank;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer2 extends Reducer<Text, Text, Text, Text> {

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
	InterruptedException {

		String links = "";
		float sumShareOtherPageRanks = (float) 0.0;
		
	    Configuration conf = context.getConfiguration();
	 	    
	    Integer NODES = Integer.parseInt(conf.get("Num_Nodes"));

		for (Text value : values) {

			String content = value.toString();

			if (content.startsWith(PageRankMain.LINKS_SEPARATOR)) 
			{
				links += content.substring(PageRankMain.LINKS_SEPARATOR.length());
			} 
			else 
			{

				String[] split = content.split("\t");
				float pageRank = Float.valueOf(split[0]);
				int totalLinks = Integer.parseInt(split[1]);
				
				//Add the other PageRank Values for a particular link
				
				sumShareOtherPageRanks += (pageRank / totalLinks);
			}

		}

		//Page Rank Formula
	
		float newRank = PageRankMain.DAMPING_FACTOR * sumShareOtherPageRanks + ((float) (1 - PageRankMain.DAMPING_FACTOR))/NODES;
		
		//Filter the dead ends
		
		//if (!links.equals("")) -- Kept it commented in order to match with sample output
		{
			context.write(key, new Text(newRank + "\t" + links));
		}
	}

}