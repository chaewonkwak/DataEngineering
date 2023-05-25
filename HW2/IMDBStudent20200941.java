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

import HW2.Movie;

public class IMDBStudent20200941 
{
	public static class RateComparator implements Comparator<Movie> 
	{
		public int compare(Movie x, Movie y) {
			if (x.rating > y.rating) return 1;
			if (x.rating < y.rating) return -1;
			return 0;
		}
	}
	public static void insertMovie(PriorityQueue q, String title, double r, int topK)
	{
		Movie head = (Movie)q.peek();
		if (q.size() < topK || head.rating < r) {
			Movie mv = new Movie(title, r);
			q.add(mv);
			if(q.size() > topK) q.remove();
		}
	}
			
	public static class IMDBMapper extends Mapper<Object, Text, IntWritable, Text>
	{
		IntWritable id = new IntWritable();
		Text title = new Text();
		Text rating = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = value.toString().split("::");
			if (tokens.length == 3) { // Movies.dat
				if (tokens[2].contains("Fantasy")){
						id.set(Integer.parseInt(tokens[0]));
						String tmp = tokens[1] + "::M";
						title.set(tmp);
						context.write(id, title);
				}
			} else if (tokens.length == 4) { // Ratings.dat
				id.set(Integer.parseInt(tokens[1]));
				rating.set(tokens[2]);
				context.write(id, rating);
			}

		}
	}

	public static class IMDBReducer extends Reducer<IntWritable, Text, Text, DoubleWritable> 
	{
		private PriorityQueue<Movie> queue;
		private Comparator<Movie> comp = new RateComparator();
		private int topK;
		
		Text rslt = new Text();
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int sum = 0;
			int count = 0;
			String title = "";
			
			for (Text val : values) 
			{
				if (val.toString().contains("::M")) {
					String[] tokens = val.toString().split("::");
					title = tokens[0];
				} else {
					sum += Integer.parseInt(val.toString());
					count++;
				}	
			}
			if (title != "") {
				double avg = sum / (double)count;
				avg = Math.round(avg*10)/10.0; // 소수점 1자리만
				insertMovie(queue, title, avg, topK);
			//	context.write(key, rslt);
			}
			
		}
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>(topK, comp);
		}
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			while(queue.size() != 0) {
				Movie mv = (Movie)queue.remove();
				context.write(new Text(mv.title), new DoubleWritable(mv.rating));
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) 
		{
			System.err.println("Usage: IMDBStudent20200941 <in> <out> <topK>");
			System.exit(2);
		}
		
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		
		Job job = new Job(conf, "IMDBStudent20200941");
		job.setJarByClass(IMDBStudent20200941.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		job.setOutputValueClass(Text.class);
		job.setOutputKeyClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		job.waitForCompletion(true);
	}
}
