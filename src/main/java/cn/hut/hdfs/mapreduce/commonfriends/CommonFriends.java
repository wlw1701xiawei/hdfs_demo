package cn.hut.hdfs.mapreduce.commonfriends;

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
import java.util.Arrays;

public class CommonFriends {
    public static class CFMapper extends Mapper<LongWritable, Text, Text, Text> {
        String[] line_split;
        Text k2 = new Text();
        Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            line_split = v1.toString().split("\t");
            Arrays.sort(line_split, 1, line_split.length);

            v2.set(line_split[0]);
            for (int i = 1; i < line_split.length; i ++) {
                for (int j = i + 1; j <line_split.length; j ++) {
                    k2.set(line_split[i] + "-" + line_split[j]);
                    context.write(k2, v2);
                }
            }
        }
    }

    public static class CFReducer extends Reducer<Text, Text, Text, IntWritable> {
        int count;
        Text k3;
        IntWritable v3 = new IntWritable();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            count = 0;
            k3 = k2;
            while (v2s.iterator().hasNext()) {
                count ++;
                v2s.iterator().next();
            }
            v3.set(count);
            context.write(k3, v3);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(CommonFriends.class);
            job.setMapperClass(CFMapper.class);
            job.setReducerClass(CFReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\共同好友问题\\input"));
            Path output = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\共同好友问题\\output");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("共同好友计数" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
