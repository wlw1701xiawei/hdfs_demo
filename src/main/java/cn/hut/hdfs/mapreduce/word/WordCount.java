package cn.hut.hdfs.mapreduce.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {
    public static class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        String[] line_split;
        Text k2 = new Text();
        IntWritable v2 = new IntWritable(1);

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            line_split = v1.toString().split("[ ?!(),.，。]");
            for (String word : line_split) {
                k2.set(word);
                context.write(k2, v2);
            }
        }
    }

    public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        int count;
        Text k3;
        IntWritable v3 = new IntWritable();

        @Override
        protected void reduce(Text k2, Iterable<IntWritable> v2s, Context context) throws IOException, InterruptedException {
            count = 0;
            while (v2s.iterator().hasNext()) {
                count ++;
                v2s.iterator().next();
            }
            k3 = k2;
            v3.set(count);
            context.write(k3, v3);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(WordCount.class);
            job.setMapperClass(WCMapper.class);
            job.setReducerClass(WCReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\单词计数问题\\input"));
            Path output = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\单词计数问题\\计数输出");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("单词计数" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
