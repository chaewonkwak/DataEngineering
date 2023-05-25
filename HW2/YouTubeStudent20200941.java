import java.io.IOException;
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

import HW2.YouTube;

public class YouTubeStudent20200941 
{
	public static class RateComparator implements Comparator<YouTube> 
	{
		public int compare(YouTube x, YouTube y) {
			if (x.rating > y.rating) return 1;
			if (x.rating < y.rating) return -1;
			return 0;
		}
	}
	public static void insertYouTube(PriorityQueue q, String category, double r, int topK)
	{
		YouTube head = (YouTube)q.peek();
		if (q.size() < topK || head.rating < r) {
			YouTube yt = new YouTube(category, r);
			q.add(yt);
			if(q.size() > topK) q.remove();
		}
	}
			
	public static class YouTubeMapper extends Mapper<Object, Text, Text, DoubleWritable>
	{
		Text category = new Text();
		DoubleWritable rating = new DoubleWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = value.toString().split("\\|");
			if (tokens.length == 7) {
				String[] genres = tokens[3].split(" & ");

				for (String genre : genres) {
					category.set(genre);
					rating.set(Double.parseDouble(tokens[6]));
					context.write(category, rating);
				}		
			}

		}
	}

	public static class YouTubeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> 
	{
		private PriorityQueue<YouTube> queue;
		private Comparator<YouTube> comp = new RateComparator();
		private int topK;
		
		Text rslt = new Text();
		
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
		{
			double sum = 0.0;
			int count = 0;
			
			for (DoubleWritable val : values) 
			{
				sum += val.get();
				count++;	
			}
			
			double avg = 0;
			if (count != 0) {	
				avg = sum / (double)count;
			}
			insertYouTube(queue, key.toString(), avg, topK);
			
		}
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<YouTube>(topK, comp);
		}
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			while(queue.size() != 0) {
				YouTube yt = (YouTube)queue.remove();
				
				double tmp = Math.round(yt.rating*10000)/10000.0;
				context.write(new Text(yt.category), new DoubleWritable(tmp));
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: YouTubeStudent20200941 <in> <out> <topK>");
			System.exit(2);
		}
		
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		
		Job job = new Job(conf, "YouTubeStudent20200941");
		job.setJarByClass(YouTubeStudent20200941.class);
		job.setMapperClass(YouTubeMapper.class);
		job.setReducerClass(YouTubeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		job.waitForCompletion(true);
	}
}
