package cn.hut.hdfs.mapreduce.srotpriority;

import cn.hut.hdfs.mapreduce.srotpriority.bean.PriorityBean;
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

public class SortPriority {
    public static class SPMapper extends Mapper<LongWritable, Text, PriorityBean, NullWritable> {
        String[] line_split;
        PriorityBean k2 = new PriorityBean();
        NullWritable v2 = NullWritable.get();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            line_split = v1.toString().split("\t");
            k2.setSP1(line_split[0]);
            k2.setSP2(line_split[1]);
            k2.setSP3(line_split[2]);
            k2.setSP4(line_split[3]);
            context.write(k2, v2);
        }
    }

    public static class SPReducer extends Reducer<PriorityBean, NullWritable, PriorityBean, NullWritable> {
        @Override
        protected void reduce(PriorityBean k2, Iterable<NullWritable> v2s, Context context) throws IOException, InterruptedException {
            while (v2s.iterator().hasNext()) {
                context.write(k2, v2s.iterator().next());
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(SortPriority.class);
            job.setMapperClass(SPMapper.class);
            job.setReducerClass(SPReducer.class);

            job.setMapOutputKeyClass(PriorityBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(PriorityBean.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job, new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\排序优先级问题\\input"));
            Path output = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\排序优先级问题\\排序输出");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("优先级排序" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
