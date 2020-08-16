package cn.hut.hadoop.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {
    public static class FilterMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        NullWritable v2 = NullWritable.get();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            context.write(v1, v2);
        }
    }

    public static class FilterRdeucer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text k2, Iterable<NullWritable> v2s, Context context) throws IOException, InterruptedException {
            for (NullWritable v2 : v2s) {
                context.write(k2, v2);
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(Main.class);
            job.setMapperClass(FilterMapper.class);
            job.setReducerClass(FilterRdeucer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);

            //在job中设置自己的自定义输出格式
            job.setOutputFormatClass(FilterOutputFormat.class);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\abuilex\\Documents\\mapreduce\\自定义OutputFormat\\input\\web域名.txt"));
            Path output = new Path("C:\\Users\\abuilex\\Documents\\mapreduce\\自定义OutputFormat\\output");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            //虽然自定义了OutputFormat，但是自定义OutputFormat继承FileOutputFormat类
            //而FileOutputFormat需要输出一个_SUCCESS文件，所以还得指定一个输出目录
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("网址过滤" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
