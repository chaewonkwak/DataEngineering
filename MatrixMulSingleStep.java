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

public class MatrixMulSingleStep
{
	public static class CompositeKeyComparator extends WritableComparator
	{
		protected CompositeKeyComparator()
		{
			super(DoubleKey.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			DoubleKey k1 = (DoubleKey)w1;
			DoubleKey k2 = (DoubleKey)w2;

			int result = k1.key.compareTo(k2.key);
			if (result == 0) {
				result = Integer.compare(k1.x, k2.x);
			}
			return result;
		}
	}
	
	public static class FirstPartitioner extends Partitioner<DoubleKey, Text>
	{
		public int getPartition(DoubleKey k, Text value, int numPartition)
		{
			return k.key.hashCode()%numPartition; // assign to the same reducer when i, j is the same.
		}
	}

	public static class FirstGroupingComparator extends WritableComparator
	{
		protected FirstGroupingComparator()
		{
			super(DoubleKey.class, true);
		}
		
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			DoubleKey k1 = (DoubleKey)w1;
			DoubleKey k2 = (DoubleKey)w2;
			
			return k1.key.compareTo(k2.key); // assign to the same value group when i, j is the same.
		}
	}

	public static class MatrixMulMapper extends Mapper<Object, Text, DoubleKey, IntWritable>
	{
		private IntWritable i_value = new IntWritable();

		// Configuration 에서 행렬 크기 정보 가져오기 : mxk X kxn
		private int m_value;
		private int k_value;
		private int n_value;

		//처리하고 있는 행렬이 A인지 B인지 (이건 파일 이름에서 가져올 것)
		private boolean isA = false;		 
		private boolean isB = false;	

		protected void setup(Context context) throws IOException, InterruptedException
		{
			Configuration conf = context.getConfiguration();
		
			m_value = conf.getInt("m", -1);
			k_value = conf.getInt("k", -1);
			n_value = conf.getInt("n", -1);
			
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			if( filename.equals("matrix_a") ) isA = true;
			if( filename.equals("matrix_b") ) isB = true;
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			int row_id = Integer.parseInt(itr.nextToken().trim());
			int col_id = Integer.parseInt(itr.nextToken().trim());
			int matrix_value = Integer.parseInt(itr.nextToken().trim());
			i_value.set(matrix_value);

			if (isA) {
				for(int i = 0; i < n_value; i++) {
					context.write(new DoubleKey(row_id + " " + i, col_id), i_value);
				}
			} else if (isB) {
				for (int i = 0; i < m_value; i++) {
					context.write(new DoubleKey(i + " " + col_id, row_id), i_value);
				}
			}
		}
	}

	public static class MatrixMulReducer extends Reducer<DoubleKey, IntWritable, Text, IntWritable> 
	{
		private IntWritable result = new IntWritable();
		private Text word = new Text();
		

		public void reduce(DoubleKey k, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			int mul = 1; // initialize
			int sum = 0;

			StringTokenizer itr = new StringTokenizer(k.key);

			int row_id = Integer.parseInt(itr.nextToken().trim());
			int col_id = Integer.parseInt(itr.nextToken().trim());
			
			int cnt = 0;
			for (IntWritable val : values) {
				if (cnt % 2 == 0) {
					mul = val.get();
				} 
				else {
					sum += mul * val.get();
					mul = 0;
				}
				cnt++;
			}
			word.set(row_id + "," + col_id);
			result.set(sum);
			context.write(word,result);
		}
	}


	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		
		int m_value = 2;
		int k_value = 2;
		int n_value = 2;

		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: MatrixMulSingleStep <in> <out> ");
			System.exit(2);
		}
		
		conf.setInt("m", m_value);
		conf.setInt("k", k_value);
		conf.setInt("n", n_value);
		
		Job job = new Job(conf, "MatrixMulSingleStep");
		job.setJarByClass(MatrixMulSingleStep.class);
		job.setMapperClass(MatrixMulMapper.class);
		job.setReducerClass(MatrixMulReducer.class);
		job.setOutputKeyClass(DoubleKey.class);
		job.setOutputValueClass(IntWritable.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		job.waitForCompletion(true);

	}
}

class DoubleKey implements WritableComparable
{
	String key; // i, j
	int x; // x

	public DoubleKey() {}
	public DoubleKey(String _key,  int _x)
	{
		key = _key;
		x = _x;
	}

	public void readFields(DataInput in) throws IOException
	{
		key = in.readUTF();
		x = in.readInt();
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(key);
		out.writeInt(x);
	}

	public int compareTo(Object o1)
	{
		DoubleKey o = (DoubleKey) o1;
		int ret = key.compareTo(o.key);
		if (ret!=0) return ret;
		return Integer.compare(x, o.x);
	}

	public String toString() { return key + " " + x; }
}

