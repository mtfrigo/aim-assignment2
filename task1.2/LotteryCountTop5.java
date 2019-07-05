package lottery;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.google.common.collect.Iterables;

/**
 * Count the occurencies of each lottery number
 */

public class LotteryCountTop5 {
	
	
	/**
	 * Mapper which extracts the lottery number and passes it to the Reducer with a single occurency 
	 */
	public static class LotteryMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String lotteryNumbers = value.toString().split(",")[1];
			//05 09 45
			StringTokenizer itr = new StringTokenizer(lotteryNumbers);
			
			while (itr.hasMoreTokens()){
					
				word.set(itr.nextToken());
				
				context.write(word, one);
								
			}
		}
	}

	/**
	 * Reducer to sum the occurencies up
	 */
	public static class LotteryReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			//05 1
			int sum = 0;
			for(IntWritable val: values){
				sum += val.get();
			}
			result.set(sum);
			context.write(key,result);
		}
	}
	
	/**
	 * Mapper which extracts the lottery number and passes it to the Reducer with a single occurency 
	 */
	public static class LotteryTop5Mapper extends Mapper<Object, Text, IntWritable, IntWritable> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] parts = value.toString().split("\t");
			String num = parts[0];
			String freq = parts[1];
			
			
			context.write(new IntWritable(Integer.parseInt(freq)), new IntWritable(Integer.parseInt(num)));
			
			
		}
	}
	
	public static class SortIntComparator extends WritableComparator {
		 
		 protected SortIntComparator() {
				super(IntWritable.class, true);
			}
		 
		 @SuppressWarnings("rawtypes")

			@Override
			public int compare(WritableComparable w1, WritableComparable w2) {
				IntWritable k1 = (IntWritable)w1;
				IntWritable k2 = (IntWritable)w2;
				
				return -1 * k1.compareTo(k2);
			}
		 
	 }
	
 
	/**
	 * Reducer to sum the occurencies up
	 */
	public static class LotteryTop5Reducer extends Reducer<IntWritable, IntWritable , IntWritable, IntWritable> {
		
		int i = 5;
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			for (IntWritable value : values) {
				if(i > 0)
				{
					context.write(value, key);
					i--;
				}
				else
					break;
				
			}
			
			
			
		}
	}
	

	/**
	 * Mapper which extracts the lottery number and passes it to the Reducer with a single occurency 
	 */
	public static class OrderByMapper extends Mapper<Object, Text, Text, Text> {
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] parts = value.toString().split("\t");
			String freq = parts[0];
			String num = parts[1];
			
			
			context.write(new Text(num), new Text(freq));
			
			
		}
	}
	
 
	
	

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Path out = new Path(args[1]);
		
		Job job1 = Job.getInstance(conf, "LotteryCount");
		job1.setJarByClass(LotteryCountTop5.class);
		
		job1.setMapperClass(LotteryMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		//job1.setCombinerClass(LotteryReducer.class); // combiner can be used since it's an associative operation defined in the reducer
		job1.setReducerClass(LotteryReducer.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		//job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		
		TextInputFormat.addInputPath(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(out, "counter"));
		
		if(!job1.waitForCompletion(true))
		{
			System.exit(1);
		}	
		
		//job 2 
		
		Configuration conf2 = new Configuration();
		
		Job job2 = Job.getInstance(conf2, "LotteryCountTop5");
		job2.setJarByClass(LotteryCountTop5.class);
		
		job2.setMapperClass(LotteryTop5Mapper.class);
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(IntWritable.class);
	    job2.setSortComparatorClass(SortIntComparator.class);
		job2.setCombinerClass(LotteryTop5Reducer.class); // combiner can be used since it's an associative operation defined in the reducer
		job2.setReducerClass(LotteryTop5Reducer.class);
		
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		
		TextInputFormat.addInputPath(job2, new Path(out, "counter"));
		TextOutputFormat.setOutputPath(job2, new Path(out, "sorted"));
		
		if(!job2.waitForCompletion(true))
		{
			System.exit(1);
		}
				
		//job 3 
		
		Configuration conf3 = new Configuration();
		
		Job job3 = Job.getInstance(conf2, "LotteryCountTop");
		job3.setJarByClass(LotteryCountTop5.class);
		
		job3.setMapperClass(OrderByMapper.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		job3.setNumReduceTasks(0);
		
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(Text.class);
		
	    job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		
		
		TextInputFormat.addInputPath(job3, new Path(out, "sorted"));
		TextOutputFormat.setOutputPath(job3, new Path(out, "top5"));
		
		if(!job3.waitForCompletion(true))
		{
			System.exit(1);
		}
		
		
		
	}

}
