package assignment1.task2;

import javafx.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.LinkedList;
import java.util.Map;
//
//import HDFSAPI;

public class Step4_2 {
	public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
		//get configuration info
		Configuration conf = Recommend.config();
		// get I/O path
		Path input = new Path(path.get("Step4_2Input"));
		Path output = new Path(path.get("Step4_2Output"));
		// delete last saved output
		HDFSAPI hdfs = new HDFSAPI(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// set job
		Job job = Job.getInstance(conf);
		job.setJarByClass(Step4_2.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Step4_RecommendMapper.class);
		job.setReducerClass(Step4_RecommendReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}

	public static class Step4_RecommendMapper extends Mapper<LongWritable, Text, Text, Text> {
		private final static Text k = new Text();
		private final static Text v = new Text();

		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] tokens = values.toString().split("\t")[1].split(" ");
			LinkedList<Pair> itemNCountList = new LinkedList<Pair>();
			LinkedList<Pair> idNScoreList = new LinkedList<Pair>();
			for (String token : tokens) {
//        		context.write(new Text(tokens[i]), new Text("shit"));
				String identifier = token.split(":")[0];
				String data = token.split(":")[1];
				if (identifier.equals("A")) {
					// id,score
					Pair idNScore = new Pair(data.split(",")[0], data.split(",")[1]);
					idNScoreList.add(idNScore);
//			        context.write(new Text(data), new Text(String.valueOf(idNScore.getKey())));
				}
				if (identifier.equals("B")) {
					// item,count
					Pair itemNCount = new Pair(data.split(",")[0], data.split(",")[1]);
					itemNCountList.add(itemNCount);
				}
//        		context.write(new Text(identifier), new Text(itemNCountList.toString()));

			}

			for (Pair itemCountPair : itemNCountList) {
				for (Pair idScorePair : idNScoreList) {
					String item = String.valueOf(itemCountPair.getKey());
					int count = Integer.parseInt(String.valueOf(itemCountPair.getValue()));
					String id = String.valueOf(idScorePair.getKey());
					float score = Float.parseFloat(String.valueOf(idScorePair.getValue()));
					float weightedScore = score * count;

					k.set(id + "," + item);
					v.set(String.valueOf(weightedScore));
					context.write(k, v);
				}
			}

		}
	}

	public static class Step4_RecommendReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			float score = 0;

			for (Text val : values) {
				score += Float.parseFloat(val.toString());
			}
			context.write(key, new Text(String.valueOf(score)));
		}
	}
}

