package cn.hut.hdfs.mapreduce.flow;

import cn.hut.hdfs.mapreduce.flow.bean.SetLoadFlow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDecreasingSort {
    public static class FDSMapper extends Mapper<LongWritable, Text, SetLoadFlow, Text> {
        String[] line_split;
        SetLoadFlow k2 = new SetLoadFlow();
        Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            line_split = v1.toString().split("\t");
            k2.setUpload(Double.valueOf(line_split[1]))
                    .setDownload(Double.valueOf(line_split[2]))
                    .setLoad(Double.valueOf(line_split[3]));
            v2.set(line_split[0]);
            context.write(k2, v2);
        }
    }

    public static class FDSReducer extends Reducer<SetLoadFlow, Text, Text, SetLoadFlow> {
        Text k3;
        SetLoadFlow v3;

        @Override
        protected void reduce(SetLoadFlow k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            while (v2s.iterator().hasNext()) {
                k3 = v2s.iterator().next();
                v3 = k2;
                context.write(k3, v3);
            }
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(FlowDecreasingSort.class);
            job.setMapperClass(FDSMapper.class);
            job.setReducerClass(FDSReducer.class);

            job.setMapOutputKeyClass(SetLoadFlow.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(SetLoadFlow.class);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\流量统计问题\\统计输出"));
            Path output = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\流量统计问题\\降序输出");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("降序排序" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
