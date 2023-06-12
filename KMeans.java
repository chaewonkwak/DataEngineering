import java.io.IOException;
import java.util.*;
import java.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans
{

	public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text>
	{
		private IntWritable one_key = new IntWritable();
		
		private int n_centers;
		private double[] center_x;
		private double[] center_y;
		
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			n_centers = conf.getInt("n_centers", -1);
			center_x = new double[n_centers];
			center_y = new double[n_centers];

			for (int i = 0; i < n_centers; i++) {
				center_x[i] = conf.getFloat("center_x_" + i, 0);
				center_y[i] = conf.getFloat("center_y_" + i, 0);
			}
		}

		// calculate distance between two points..
		public double getDist(double x1, double y1, double x2, double y2) 
		{
			double dist = (x1-x2)*(x1-x2) + (y1-y2)*(y1-y2);
			return Math.sqrt(dist);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			if(itr.countTokens() < 2) return;
			if(n_centers == 0) return;
		
			double x = Double.parseDouble(itr.nextToken().trim());
			double y = Double.parseDouble(itr.nextToken().trim());
			int cluster_idx = 0;
			
			double min = Integer.MAX_VALUE;
			for (int i = 0; i < n_centers; i++) {
				if (getDist(center_x[i], center_y[i], x, y) <= min) {
					min = getDist(center_x[i], center_y[i], x, y);
					cluster_idx = i;
				}
			}
			one_key.set(cluster_idx);
			context.write(one_key, value);
		}
	}

	public static class KMeansReducer extends Reducer<IntWritable,Text,IntWritable,Text> 
	{

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			double x_total = 0;
			double y_total = 0;
			int cnt = 0;
			Text result = new Text();
			for (Text val: values) {
				StringTokenizer itr = new StringTokenizer(val.toString());
				x_total += Double.parseDouble(itr.nextToken().trim());
				y_total += Double.parseDouble(itr.nextToken().trim());
				cnt++;
			}
			result.set((x_total/(double)cnt) + " " + (y_total/(double)cnt));
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		
		int n_centers = 2;
		int n_iter = 3;

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: KMeans <in> <out>");
			System.exit(2);
		}
		initCenters(conf, n_centers);

		for(int i = 0; i < n_iter; i++) {
			Job job = new Job(conf, "KMeans");
			job.setJarByClass(KMeans.class);
			job.setMapperClass(KMeansMapper.class);
			job.setReducerClass(KMeansReducer.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
	
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
			job.waitForCompletion(true);
			
			updateCenters(conf, n_centers); // 계산된 center로 업데이트
		}
	}
	public static void initCenters(Configuration conf, int n_centers)
	{
		conf.setInt("n_centers", n_centers);
		for (int i = 0; i < n_centers; i++) {
			conf.setFloat("center_x_" + i, (float)(1.0/(double)n_centers));  
			conf.setFloat("center_y_" + i, (float)(1.0/(double)n_centers));
		}
	}
	public static void updateCenters(Configuration conf, int n_centers) throws Exception
	{
		FileSystem dfs = FileSystem.get(conf);
		Path filenamePath = new Path("/user/bigdata/output0611_3/part-r-00000");
		FSDataInputStream in = dfs.open(filenamePath);
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		
		String line = reader.readLine();
		while(line != null) {
			StringTokenizer itr = new StringTokenizer(new String(line));
			int cluster_id = Integer.parseInt(itr.nextToken().trim());
			double x = Double.parseDouble(itr.nextToken().trim());
			double y = Double.parseDouble(itr.nextToken().trim());
			conf.setFloat("center_x_"+ cluster_id, (float)x);  // 새롭게 계산된 center value를 읽어서 저장
			conf.setFloat("center_y_"+ cluster_id, (float)y);  // 새롭게 계산된 center value를 읽어서 저장
			line = reader.readLine();
		}
	}
}
	
