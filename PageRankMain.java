package PageRank;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;


public class PageRankMain {
    
    
    // temp variables
    public static String LINKS_SEPARATOR = "|";
    
    // configuration values
    public static float DAMPING_FACTOR = (float) 0.85;
    public static int ITERATIONS = 8;
    public static String IN_PATH = "";
    public static String OUT_PATH = "";
    
    
    /*
     * Main Function of PageRank algorithm.
     */
    
    public static void main(String[] args) throws Exception {
        
        try {
             // Set input and output file paths
        	
                 IN_PATH = args[0];
                 OUT_PATH = args[1];
                
                 Path in = new Path(PageRankMain.IN_PATH);
                 
                 Configuration conf_fs = new Configuration();
               
        // delete output path if it exists already
        FileSystem fs = in.getFileSystem(conf_fs);
        
        
        if (fs.exists(new Path(PageRankMain.OUT_PATH)))
            fs.delete(new Path(PageRankMain.OUT_PATH), true);
        
        
        
        String inPath = null;
        String lastOutPath = null;
        PageRankMain pagerank = new PageRankMain();
        
        
        boolean isCompleted = pagerank.job0(IN_PATH, OUT_PATH + "/temp/num_nodes");
        if (!isCompleted) {
            System.exit(1);
        }
        
        //Add the information to a file about the number of nodes
        copyMerge(fs, new Path(OUT_PATH + "/temp/num_nodes") , fs, 
        		new Path(OUT_PATH + "/num_nodes")
        		, false, new Configuration(), null);
        
        
        //Read the count of initial nodes from file
        
        Path inFile = new Path(OUT_PATH+"/num_nodes");
        FSDataInputStream input = fs.open(inFile);
        @SuppressWarnings("deprecation")
		String num_nodes = input.readLine().trim();
        
        isCompleted = pagerank.job1(IN_PATH, OUT_PATH + "/temp/iter0", num_nodes);
        if (!isCompleted) {
            System.exit(1);
        }
        
        
        
        
        for (int runs = 0; runs < ITERATIONS; runs++) {
            inPath = OUT_PATH + "/temp/iter" + runs;
            lastOutPath = OUT_PATH + "/temp/iter" + (runs + 1);
            isCompleted = pagerank.job2(inPath, lastOutPath,num_nodes);
            
            //Sort the output for the 1st and 8th Iterations

            if (runs == 0 || runs == 7)
            {
            	System.out.println("Running Job#3 (rank ordering) ...");
                isCompleted = pagerank.job3(lastOutPath, OUT_PATH + "/temp/result/iter"+(runs+1), num_nodes);
                copyMerge(fs, new Path(OUT_PATH + "/temp/result/iter"+(runs+1)) , fs, 
                		new Path(OUT_PATH + "/iter"+ (runs + 1)+".out")
                		, false, new Configuration(), null);
                
                if (!isCompleted) {
                    System.exit(1);
                    
                  
                }
            }
            
            if (!isCompleted) {
                System.exit(1);
            }
        }
        

        System.exit(0);
        
        }
        catch (Exception e) {
            System.out.println(e.getMessage().toString());
        }  
	   
   
        }
    

    //Jobs Setting Parameters
    
    //Job 0 -- Number of nodes counter
    
    private boolean job0(String in, String out) throws IOException, 
    ClassNotFoundException, 
    InterruptedException {
    	
    	Job job = Job.getInstance(new Configuration(), "Job #0");
        job.setJarByClass(PageRankMain.class);
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(PageRankMapper0.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Integer.class);
        job.setOutputValueClass(NullWritable.class);
        job.setReducerClass(PageRankReducer0.class);
        job.setNumReduceTasks(1);
        return job.waitForCompletion(true);
	}

    //Job 1 -- Initial Page Rank Setter

	public boolean job1(String in, String out, String num_nodes) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
    	
		Configuration conf = new Configuration();
		
        conf.set("Num_Nodes", num_nodes);
		
        Job job = Job.getInstance(conf, "Job #1");

        job.setJarByClass(PageRankMain.class);
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankMapper1.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankReducer1.class);
        return job.waitForCompletion(true);
     
    }

    //Job 2 -- Iterative Page Rank Calculator
    
     public boolean job2(String in, String out,String num_nodes) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
		Configuration conf = new Configuration();
		
        conf.set("Num_Nodes", num_nodes);
    	 
        Job job = Job.getInstance(conf, "Job #2");
        job.setJarByClass(PageRankMain.class);
        
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankMapper2.class);
        
        // output / reducer
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankReducer2.class);

        return job.waitForCompletion(true);
        
    }


    
   // Page Rank Sorter
     
    public boolean job3(String in, String out, String num_nodes) throws IOException, 
                                                      ClassNotFoundException, 
                                                      InterruptedException {
        
    	
		Configuration conf = new Configuration();
		
        conf.set("Num_Nodes", num_nodes);
    	
        Job job = Job.getInstance(conf, "Job #3");
        job.setJarByClass(PageRankMain.class);
        
        // input / mapper
        FileInputFormat.addInputPath(job, new Path(in));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(PageRankMapper3.class);
        job.setSortComparatorClass(Sorter.class);
        // output
        FileOutputFormat.setOutputPath(job, new Path(out));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setReducerClass(PageRankReducer3.class);
        job.setNumReduceTasks(1);

        return job.waitForCompletion(true);
        
    }
    
    // Function to copy the output of the Reducer to a different location
    
    public static boolean copyMerge(FileSystem srcFS, Path srcDir, 
    		                                  FileSystem dstFS, Path dstFile, 
    		                                  boolean deleteSource,
    		                                  Configuration conf, String addString) throws IOException {
    	
    		    
    		
    		    if (!srcFS.getFileStatus(srcDir).isDirectory())
    		      return false;
    		   
    		    FSDataOutputStream out = dstFS.create(dstFile);
    		    
    		    try {
    		      FileStatus contents[] = srcFS.listStatus(srcDir);
    		      Arrays.sort(contents);
    		      for (int i = 0; i < contents.length; i++) {
    		        if (contents[i].isFile()) {
    		          FSDataInputStream in = srcFS.open(contents[i].getPath());
    		          try {
    		            IOUtils.copyBytes(in, out, conf, false);
    		            if (addString!=null)
    		              out.write(addString.getBytes("UTF-8"));
    		                
    		          } finally {
    		            in.close();
    		          } 
    		        }
    		      }
    		    } finally {
    		      out.close();
    		    }
    		    
    		
    		      return true;
    		    
    		  } 
    
}
