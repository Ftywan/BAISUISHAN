package assignment1.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
//
//import HDFSAPI;

public class Step4_1 {
	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
		//get configuration info
		Configuration conf = Recommend.config();
		// get I/O path
		Path input1 = new Path(path.get("Step4_1Input1"));
		Path input2 = new Path(path.get("Step4_1Input2"));
		Path output = new Path(path.get("Step4_1Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);

		Job job = new Job(conf, "Step4");
		job.setJarByClass(Step4_1.class);

		MultipleInputs.addInputPath(job, input1,
				TextInputFormat.class, Step4_PartialMultiplyMapper1.class);
		MultipleInputs.addInputPath(job, input2,
				TextInputFormat.class, Step4_PartialMultiplyMapper2.class);

		job.setCombinerClass(Step4_AggregateReducer.class);
		job.setReducerClass(Step4_AggregateReducer.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, output);
		job.waitForCompletion(true);

	}

	public static class Step4_PartialMultiplyMapper1 extends Mapper<LongWritable, Text, IntWritable, Text> {

		IntWritable k = new IntWritable();
		Text v = new Text();

		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] tokens = values.toString().split("\t");
			k.set(Integer.parseInt(tokens[0]));
			v.set("A:" + tokens[1]);
			context.write(k, v);
		}
	}

	public static class Step4_PartialMultiplyMapper2 extends Mapper<LongWritable, Text, IntWritable, Text> {

		IntWritable k = new IntWritable();
		Text v = new Text();

		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] tokens = values.toString().split("\t");
			k.set(Integer.parseInt(tokens[0]));
			v.set("B:" + tokens[1]);
			context.write(k, v);
		}

	}

	public static class Step4_AggregateReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String value = "";

			for (Text val : values) {
				value += val.toString();
				value += " ";
			}
			context.write(key, new Text(value.trim()));

		}
	}
}

