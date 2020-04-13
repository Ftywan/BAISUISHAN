package assignment1.task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Map;
//
//import HDFSAPI;

public class Step5 {
	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
		//get configuration info
		Configuration conf = Recommend.config();
		// I/O path
		Path input = new Path(path.get("Step5Input"));
		Path output = new Path(path.get("Step5Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step5.class);

		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step5_FilterSortMapper.class);
		job.setReducerClass(Step5_FilterSortReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}

	public static class Step5_FilterSortMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
		//		private String flag;
		private FloatWritable k = new FloatWritable();
		private Text v = new Text();

		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(values.toString());
			String id = tokens[0];
			String item = tokens[1];
			String score = tokens[2];
			if (Integer.parseInt(id) == Recommend.ID) {
				k.set(-1 * Float.parseFloat(score));
				v.set(item);
				context.write(k, v);
			}
		}
	}

	public static class Step5_FilterSortReducer extends Reducer<FloatWritable, Text, FloatWritable, Text> {
		@Override
		public void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(new FloatWritable(-1 * key.get()), val);
			}

		}
	}
}

