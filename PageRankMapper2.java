package PageRank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



import java.io.IOException;

public class PageRankMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


		// extract values from the current line
		if (value.toString().split("\t").length>2)
		{
			String page = value.toString().split("\t")[0];
			String pageRank = value.toString().split("\t")[1];
			String links = value.toString().split("\t")[2];

			String[] allOtherPages = links.split("~");
			for (String otherPage : allOtherPages) { 

				Text pageRankWithTotalLinks = new Text(pageRank + "\t" + allOtherPages.length);

				context.write(new Text(otherPage), pageRankWithTotalLinks); 
			}

			// put the original links so the reducer is able to produce the correct output


			context.write(new Text(page), new Text(PageRankMain.LINKS_SEPARATOR + links));
		}
	}

}