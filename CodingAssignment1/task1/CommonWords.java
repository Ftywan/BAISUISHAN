package assignment1.task1;//assignment1 task1 commonwords java
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;


public class CommonWords {

    //Remove Stopwords
    public static class TokenizerWCMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        Set<String> stopwords = new HashSet<String>();

        @Override
        protected void setup(Context context) {
            try {
                Path path = new Path("src/main/java/assignment1/task1/data/stopwords.txt");
                FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(
                        fs.open(path)));
                String word = null;
                while ((word = br.readLine()) != null) {
                    stopwords.add(word);

                }

            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().toLowerCase());
                if (stopwords.contains(word.toString()))
                    continue;
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable count = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }

    //Mapper1
    public static class Mapper1 extends Mapper<Text, Text, Text, Text> {
        private Text frequency = new Text();
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            frequency.set(value.toString() + "_s1");
            context.write(key, frequency);
        }
    }

    //Mapper2
    public static class Mapper2 extends Mapper<Text, Text, Text, Text> {
        private Text frequency = new Text();
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            frequency.set(value.toString() + "_s2");
            context.write(key, frequency);
        }
    }

    //Get the number of common words reduce
    public static class CountCommonReducer extends
            Reducer<Text, Text, Text, IntWritable> {
        private IntWritable commonCount = new IntWritable();

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            int stateCount = 0;
            int currentCommonCount = Integer.MAX_VALUE;
            for (Text val : values) {
                stateCount ++;
                int frequency = Integer.parseInt(val.toString().split("_")[0]);
                if (frequency < currentCommonCount) {
                    currentCommonCount = frequency;
                }
            }
            if (stateCount == 2) {
                commonCount.set(currentCommonCount);
                context.write(key, commonCount);
            }
        }

    }

//sort the result
public static class SortMapper extends Mapper<Text, Text, IntWritable, Text> {
    private IntWritable count = new IntWritable();
    public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        count.set(-1 * Integer.parseInt(value.toString()));
        context.write(count, key);
    }
}

public static class SortReducer extends
        Reducer<IntWritable, Text, IntWritable, Text> {
    static int count = 0;
    public void reduce(IntWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            if (count < 15) {
                key.set(-1 * key.get());
                context.write(key, val);
                count ++;
            }
        }
    }
}




public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
        .getRemainingArgs();

//        conf.set("mapred.textoutputformat.separator", ";");

        if (otherArgs.length != 6) {
        System.err
        .println("Usage: Assignment1.Task1.CommonWords <input1> <output1> <input2> <output2> "
        + "<output3> <output4>");
        System.exit(2);
        }

        Job job1 = new Job(conf, "WordCount1");
        job1.setJarByClass(CommonWords.class);
        job1.setMapperClass(TokenizerWCMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
        job1.waitForCompletion(true);

        Job job2 = new Job(conf, "WordCount2");
        job2.setJarByClass(CommonWords.class);
        job2.setMapperClass(TokenizerWCMapper.class);
        job2.setCombinerClass(IntSumReducer.class);
        job2.setReducerClass(IntSumReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
        job2.waitForCompletion(true);

        Job job3 = new Job(conf, "Count words in common");
        job3.setJarByClass(CommonWords.class);
        job3.setReducerClass(CountCommonReducer.class);
        MultipleInputs.addInputPath(job3, new Path(otherArgs[1]),
        KeyValueTextInputFormat.class, Mapper1.class);
        MultipleInputs.addInputPath(job3, new Path(otherArgs[3]),
        KeyValueTextInputFormat.class, Mapper2.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        job3.setNumReduceTasks(1);
        FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));
        job3.waitForCompletion(true);

        Job job4 = new Job(conf, "sort");
        job4.setJarByClass(CommonWords.class);
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
        job4.setMapperClass(SortMapper.class);
        job4.setReducerClass(SortReducer.class);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Text.class);
        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job4, new Path(otherArgs[4]));
        FileOutputFormat.setOutputPath(job4, new Path(otherArgs[5]));
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
        }

}