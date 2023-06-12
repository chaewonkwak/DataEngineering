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

public class ReduceSideJoin2
{
	public static class CompositeKeyComparator extends WritableComparator
	{
		protected CompositeKeyComparator()
		{
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2)
		{
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;

			int result = k1.joinKey.compareTo(k2.joinKey);
			if ( result == 0 ) {
				result = -1*k1.tableName.compareTo(k2.tableName);
			}
			return result;
		}
	}

	public static class FirstPartitioner extends Partitioner<DoubleString, Text>
	{
		public int getPartition(DoubleString key, Text value, int numPartition)
		{
			return key.joinKey.hashCode()%numPartition; // assign to same reducer when the joinKey is the same.
		}
	}

	public static class FirstGroupingComparator extends WritableComparator
	{
		protected FirstGroupingComparator()
		{
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) 
		{
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;

			return k1.joinKey.compareTo(k2.joinKey); // assign to a same value group when the joinKey is the same.
		}
	}

	public static class ReduceSideJoin2Mapper extends Mapper<Object, Text, DoubleString, Text>
	{
		boolean fileA = true;
			
		protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();

			if( filename.indexOf("relation_a")!=-1) fileA = true;
			else fileA = false;
		}

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			Text outputValue = new Text();
			String joinKey = "";
			String tableName = "";
			String o_value = "";
			if (fileA) {
				tableName = "A";
				o_value = itr.nextToken() + "," + itr.nextToken();
				joinKey = itr.nextToken();
			}
			else {
				tableName = "B";
				joinKey = itr.nextToken();
				o_value = itr.nextToken();
			}
			outputValue.set(o_value);
			context.write(new DoubleString(joinKey, tableName), outputValue);
		}

	}

	public static class ReduceSideJoin2Reducer extends Reducer<DoubleString, Text, Text, Text> 
	{
		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Text reduce_key = new Text();
			Text reduce_result = new Text();
			String description = "";
			ArrayList<String> buffer = new ArrayList<String>();

			for (Text val : values) {
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				StringTokenizer key_itr = new StringTokenizer(key.toString(), " ");
				key_itr.nextToken();
				String file_type = key_itr.nextToken();
				if (file_type.equals("B")) {
					description = val.toString();
				}
				else {
					reduce_key.set(itr.nextToken());
					reduce_result.set(itr.nextToken() + " " + description);
					context.write(reduce_key, reduce_result);
				}
				
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ReduceSideJoin2 <in> <output> ");
			System.exit(2);
		}
		Job job = new Job(conf, "ReduceSideJoin2");
		job.setJarByClass(ReduceSideJoin2.class);
		job.setMapperClass(ReduceSideJoin2Mapper.class);
		job.setReducerClass(ReduceSideJoin2Reducer.class);

		job.setOutputKeyClass(DoubleString.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
			
	}
}

class DoubleString implements WritableComparable
{
	String joinKey = new String();
	String tableName = new String();

	public DoubleString() {}
	public DoubleString(String _joinKey, String _tableName)
	{
		joinKey = _joinKey;
		tableName = _tableName;
	}

	public void readFields(DataInput in) throws IOException
	{
		joinKey = in.readUTF();
		tableName = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(joinKey);
		out.writeUTF(tableName);
	}

	public int compareTo(Object o1)
	{
		DoubleString o = (DoubleString) o1;
		int ret = joinKey.compareTo(o.joinKey);
		if(ret!=0) return ret;
		return -1*tableName.compareTo(o.tableName);
	}
	
	public String toString() { return joinKey + " " + tableName; }
}


