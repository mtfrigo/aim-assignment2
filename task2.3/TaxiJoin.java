package taxi;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.util.StringTokenizer;

import lottery.LotteryCountTop5;
import lottery.LotteryCountTop5.LotteryMapper;
import lottery.LotteryCountTop5.LotteryReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * MapReduce example to join taxi trip data with fuel prices.
 */
public class TaxiJoin {
	
	public static class CompositeWritable implements Writable, WritableComparable<CompositeWritable>{

		Text table;
		DoubleWritable distance;
		DoubleWritable earnings;
		DoubleWritable buffaloPrice;

		public CompositeWritable(Text table, DoubleWritable distance, DoubleWritable earnings) {
			this.table = table;
			this.distance = distance;
			this.earnings = earnings;
			this.buffaloPrice = new DoubleWritable();
		}
		
		public CompositeWritable(Text table, DoubleWritable buffaloPrice) {
			this.table = table;
			this.buffaloPrice = buffaloPrice;
			
			this.distance = new DoubleWritable();
			this.earnings = new DoubleWritable();
		}

		public CompositeWritable() {
			this.table = new Text();
			this.distance = new DoubleWritable();
			this.earnings = new DoubleWritable();
			this.buffaloPrice = new DoubleWritable();
		}

		@Override
		public String toString() {
			return this.distance.toString() + "," + this.earnings.toString() + "," + this.buffaloPrice.toString();
		}

		@Override
		public int compareTo(CompositeWritable o) {
			// TODO Auto-generated method stub

			if(o == null) {
				return 0;
			}

			int cmp = this.distance.compareTo(o.distance);

			if(cmp != 0) {
				return cmp;
			}
			else {
				int cmp2 =  this.earnings.compareTo(o.earnings);
				if(cmp2 != 0) {
					return cmp2;
				}
				else {
					return this.buffaloPrice.compareTo(o.buffaloPrice);
				}
			}
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub

			this.table.readFields(arg0);
			this.distance.readFields(arg0);
			this.earnings.readFields(arg0);
			this.buffaloPrice.readFields(arg0);

		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub

			this.table.write(arg0);
			this.distance.write(arg0);
			this.earnings.write(arg0);
			this.buffaloPrice.write(arg0);

		}
	}
	
	public static class AggMapper extends Mapper<Object, Text, Text, CompositeWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String dropOffDate = value.toString().split(",")[1];
			String distance = value.toString().split(",")[6];
			String earnings = value.toString().split(",")[7];
			
			CompositeWritable cw = new CompositeWritable(new Text(), new DoubleWritable(Double.parseDouble(distance)), new DoubleWritable(Double.parseDouble(earnings)));
			
			context.write(new Text(dropOffDate), cw);
		}
	}
	
	public static class AggReducer extends Reducer<Text, CompositeWritable, Text, CompositeWritable> {
		
		public void reduce(Text key, Iterable<CompositeWritable> values, Context context) throws IOException, InterruptedException {
			
			CompositeWritable newValue = new CompositeWritable();
			
			for(CompositeWritable value: values) {
				
				newValue.distance.set(newValue.distance.get() + value.distance.get());
				newValue.earnings.set(newValue.earnings.get() + value.earnings.get());
				
			}
			
			context.write(key, newValue);
			
		}
		
	}
	
	public static class TaxiMapper extends Mapper<Object, Text, Text, CompositeWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String dropOffDate = value.toString().split(",")[0];
			String distance = value.toString().split(",")[1];
			String earnings = value.toString().split(",")[2];
			
			DoubleWritable distanceWritable = new DoubleWritable(Double.parseDouble(distance));
			DoubleWritable earningsWritable = new DoubleWritable(Double.parseDouble(earnings));

			CompositeWritable cw = new CompositeWritable(new Text("taxi"), distanceWritable, earningsWritable);

			context.write(new Text(dropOffDate), cw);
		}
	}

	public static class FuelMapper extends Mapper<Object, Text, Text, CompositeWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String businessDay = value.toString().split(",")[0];
			DoubleWritable buffaloPrice = new DoubleWritable(Double.parseDouble(value.toString().split(",")[4]));
			
			context.write(new Text(businessDay), new CompositeWritable(new Text("fuel"), buffaloPrice));
		}
	}

	public static class TaxiJoinReducer extends Reducer<Text, CompositeWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<CompositeWritable> values, Context context) throws IOException, InterruptedException {
			
			Double distance = null;
			Double earnings = null;
			Double price = null;

			for(CompositeWritable value: values) {
				
				if(value.table.toString().equals("taxi")) {
					distance = value.distance.get();
					earnings = value.earnings.get();
				}
				else {
					price = value.buffaloPrice.get();
				}
			}
			
			if(price != null && earnings != null && distance != null) {
			
			DoubleWritable newValue = new DoubleWritable(earnings - (price * (distance / 21.3)));

			context.write(key, newValue);
			
			}

		}

	}
	
	/**
	 * Delimiter used to separate values in this global task context
	 */
	public static final String DELIMITER = ",";

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// set global delimiter for this config, very useful when performing multiple MapReduce jobs
		conf.set("mapred.textoutputformat.separator", DELIMITER);
		
		// TODO

		Path out = new Path(args[2]);
		
		Job job1 = Job.getInstance(conf, "TaxiAggregate");
		job1.setJarByClass(TaxiJoin.class);
		
		job1.setMapperClass(AggMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(CompositeWritable.class);
		//job1.setCombinerClass(LotteryReducer.class); // combiner can be used since it's an associative operation defined in the reducer
		job1.setReducerClass(AggReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(CompositeWritable.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.addInputPath(job1, new Path(args[0]));
		TextOutputFormat.setOutputPath(job1, new Path(out, "aggregate"));
		
		if(!job1.waitForCompletion(true))
		{
			System.exit(1);
		}	
		
		
		
		Configuration conf2 = new Configuration();
		// set global delimiter for this config, very useful when performing multiple MapReduce jobs
		conf2.set("mapred.textoutputformat.separator", DELIMITER);
		
		Job job2 = Job.getInstance(conf2, "TaxiJoin");
		job2.setJarByClass(TaxiJoin.class);

		job2.setReducerClass(TaxiJoinReducer.class);
		
		job2.setOutputKeyClass(Text.class);
		
		job2.setMapOutputValueClass(CompositeWritable.class);
		job2.setOutputValueClass(DoubleWritable.class);
		
		
		
		MultipleInputs.addInputPath(job2, new Path(out, "aggregate"), TextInputFormat.class, TaxiMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, FuelMapper.class);

		TextOutputFormat.setOutputPath(job2, new Path(out, "join"));

		//job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		if(!job2.waitForCompletion(true))
		{
			System.exit(1);
		}	
	}

}