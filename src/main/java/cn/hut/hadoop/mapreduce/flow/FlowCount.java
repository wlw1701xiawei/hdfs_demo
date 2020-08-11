package cn.hut.hdfs.mapreduce.flow;

import cn.hut.hdfs.mapreduce.flow.bean.FlowBean;
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

public class FlowCount {
    public static class FCMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
        String[] line_split;
        Text k2 = new Text();
        FlowBean v2 = new FlowBean();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            line_split = v1.toString().split("\t");
            k2.set(line_split[0]);
            v2.setUpload(Double.parseDouble(line_split[1]))
                    .setDownload(Double.parseDouble(line_split[2]));
            context.write(k2, v2);
        }
    }

    public static class FCReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        Text k3;
        FlowBean v3 = new FlowBean();

        @Override
        protected void reduce(Text k2, Iterable<FlowBean> v2s, Context context) throws IOException, InterruptedException {
            k3 = k2;
            v3.setUpload(0.0).setDownload(0.0);
            while (v2s.iterator().hasNext()) {
                v3.addFlowBean(v2s.iterator().next());
            }
            v3.setLoad();
            context.write(k3, v3);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(FlowCount.class);
            job.setMapperClass(FCMapper.class);
            job.setReducerClass(FCReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FlowBean.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\流量统计问题\\input"));
            Path output = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\流量统计问题\\统计输出");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("流量统计" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
