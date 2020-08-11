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

public class WordDecreasingSort {
    public static class WSMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        String[] line_split;
        IntWritable k2 = new IntWritable();
        Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            line_split = v1.toString().split("\t");
            k2.set(Integer.valueOf(line_split[1]));
            v2.set(line_split[0]);
            context.write(k2, v2);
        }
    }

    public static class WSReducer extends Reducer<IntWritable, Text, Text,IntWritable> {
        @Override
        protected void reduce(IntWritable k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            while (v2s.iterator().hasNext()) {
                context.write(v2s.iterator().next(), k2);
            }
        }
    }

    public static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        //重写比较器，进行降序排序
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return - super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(WordDecreasingSort.class);
            job.setMapperClass(WSMapper.class);
            job.setReducerClass(WSReducer.class);

            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //设置比较器类
            job.setSortComparatorClass(IntWritableDecreasingComparator.class);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\单词计数问题\\计数输出"));
            Path output = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\单词计数问题\\排序输出");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("单词出现频率排序" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
