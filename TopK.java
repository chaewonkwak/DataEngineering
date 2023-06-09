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

public class TopK
{
	public static class EmpComparator implements Comparator<Emp> {
		public int compare(Emp x, Emp y) {
			if (x.salary > y.salary) return 1;
			if (x.salary < y.salary) return -1;
			return 0;
		}
	}

	public static void insertEmp(PriorityQueue q, int id, int salary, String dept_id, String emp_info, int topK) {
		Emp emp_head = (Emp)q.peek();
		if (q.size() < topK || emp_head.salary < salary) {
			Emp emp = new Emp(id, salary, dept_id, emp_info);
			q.add(emp);
			if (q.size() > topK) q.remove();
		}
	}

	// NullWritable = 길이가 0인 no-data를 의미. Key혹은 value가 의미 없을 시 사용..
	public static class TopKMapper extends Mapper<Object, Text, Text, NullWritable>
	{
		private PriorityQueue<Emp> queue;
		private Comparator<Emp> comp = new EmpComparator();
		private int topK;
			
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Emp>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			while(queue.size() != 0) {
				Emp emp = (Emp)queue.remove();
				context.write(new Text(emp.getString()), NullWritable.get());
			}
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			int emp_id = Integer.parseInt(itr.nextToken().trim());
			String dept_id = itr.nextToken().trim();
			int salary = Integer.parseInt(itr.nextToken().trim());
			String emp_info = itr.nextToken().trim();
			insertEmp(queue, emp_id, salary, dept_id, emp_info, topK);
		}
	}

	public static class TopKReducer extends Reducer<Text, NullWritable, Text, Writable> 
	{
		private PriorityQueue<Emp> queue;
		private Comparator<Emp> comp = new EmpComparator();
		private int topK;
	
		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Emp>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			while(queue.size() != 0) {
				Emp emp = (Emp)queue.remove();
				context.write(new Text(emp.getString()), NullWritable.get());
			}
		}

		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(key.toString(), "|");
			int emp_id = Integer.parseInt(itr.nextToken().trim());
			String dept_id = itr.nextToken().trim();
			int salary = Integer.parseInt(itr.nextToken().trim());
			String emp_info = itr.nextToken().trim();
			insertEmp(queue, emp_id, salary, dept_id, emp_info, topK);
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		
		int topK = 3;

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopK <in> <output> ");
			System.exit(2);
		}
		conf.setInt("topK", topK);
		Job job = new Job(conf, "TopK");
		job.setJarByClass(TopK.class);
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}
}

class Emp {
	public int id;
	public int salary;
	public String dept_id;
	public String emp_info;

	public Emp(int _id, int _salary, String _dept_id, String _emp_info) {
		this.id = _id;
		this.salary = _salary;
		this.dept_id = _dept_id;
		this.emp_info = _emp_info;
	}

	public String getString() {
		return id +"|" + dept_id + "|" + salary + "|" + emp_info;
	}
}
