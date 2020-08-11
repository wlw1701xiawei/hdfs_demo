package cn.hut.mapreduce.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class MergeFiles {
    public static class MergeFilesMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
        @Override
        protected void map(Text k1, BytesWritable v1, Context context) throws IOException, InterruptedException {
            context.write(k1, v1);
        }
    }

    public static class MergeFilesReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
        @Override
        protected void reduce(Text k2, Iterable<BytesWritable> v2s, Context context) throws IOException, InterruptedException {
            for (BytesWritable v2 : v2s) {
                context.write(k2, v2);
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(MergeFiles.class);
            job.setMapperClass(MergeFilesMapper.class);
            job.setReducerClass(MergeFilesReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(BytesWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(BytesWritable.class);

            //设置输入的inputFormat
            job.setInputFormatClass(MergeFilesInputFormat.class);
            //设置输出的outputFormat
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\abuilex\\Documents\\mapreduce\\单词计数问题\\input"));
            Path output = new Path("C:\\Users\\abuilex\\Documents\\mapreduce\\单词计数问题\\合并文件");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("文件合并" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
