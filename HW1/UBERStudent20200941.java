import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20200941 
{

	public static class UBERMapper extends Mapper<Object, Text, Text, Text>
	{
		private Text word = new Text();
		private Text num = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] tokens = value.toString().split(",");
			if (tokens.length == 4) {
				String[] date = tokens[1].split("/");
				Calendar cal = Calendar.getInstance();
				cal.set(Integer.parseInt(date[2]), Integer.parseInt(date[0]), Integer.parseInt(date[1]));
				int dayOfWeekNum = cal.get(Calendar.DAY_OF_WEEK);
				if (dayOfWeekNum == 1) word.set(tokens[0] + ",SUN");
				if (dayOfWeekNum == 2) word.set(tokens[0] + ",MON");
				if (dayOfWeekNum == 3) word.set(tokens[0] + ",TUE");
				if (dayOfWeekNum == 4) word.set(tokens[0] + ",WED");
				if (dayOfWeekNum == 5) word.set(tokens[0] + ",THR");
				if (dayOfWeekNum == 6) word.set(tokens[0] + ",FRI");
				if (dayOfWeekNum == 7) word.set(tokens[0] + ",SAT");
				
				num.set(tokens[2] + "," + tokens[3]);
				
				context.write(word, num);
			}
		}
	}

	public static class UBERReducer extends Reducer<Text,Text,Text,Text> 
	{
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			int trips = 0;
			int vehicles = 0;
			
			for (Text val : values) {
				String[] tokens = val.toString().split(",");
				trips += Integer.parseInt(tokens[1]);
				vehicles += Integer.parseInt(tokens[0]);
			}
			result.set(trips + "," + vehicles);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: UBERStudent20200941 <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBERStudent20200941");
		job.setJarByClass(UBERStudent20200941.class);
		job.setMapperClass(UBERMapper.class);
		job.setCombinerClass(UBERReducer.class);
		job.setReducerClass(UBERReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
