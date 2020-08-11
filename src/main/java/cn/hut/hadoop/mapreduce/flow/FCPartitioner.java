package cn.hut.hdfs.mapreduce.flow;

import cn.hut.hdfs.mapreduce.flow.bean.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class FCPartitioner extends Partitioner<Text, FlowBean> {
    private static HashMap<String, Integer> ruler = new HashMap<>();
    private Integer res;

    static {
        ruler.put("135", 1);
        ruler.put("136", 1);
        ruler.put("138", 1);
        ruler.put("150", 2);
        ruler.put("157", 2);
        ruler.put("177", 3);
    }
    @Override
    public int getPartition(Text k2, FlowBean v2, int numPartitions) {
        res = null;
        res = ruler.get(k2.toString().substring(0, 3));
        if (res == null) {
            res = 0;
        }
        return res;
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(FlowCount.class);
            job.setMapperClass(FlowCount.FCMapper.class);
            //设置分区类
            job.setPartitionerClass(FCPartitioner.class);
            job.setReducerClass(FlowCount.FCReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FlowBean.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FlowBean.class);

            //根据分区类的规则，配置几个分区
            job.setNumReduceTasks(4);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\流量统计问题\\input"));
            Path output = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\流量统计问题\\分区统计输出");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("流量分区统计" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}