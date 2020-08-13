package cn.hut.hadoop.mapreduce.groupingcomparator;

import cn.hut.hadoop.mapreduce.groupingcomparator.bean.OrderBean;
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

/**
 * 输出每个订单中最高的商品的交易额及其订单id
 * 订单的数据结构: id(String), 商品id(String), 成交金额(Double)
 */
public class Main {
    public static class GCMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
        String[] line_split;
        OrderBean k2 = new OrderBean();
        NullWritable v2 = NullWritable.get();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            line_split = v1.toString().split("\t");
            k2.setId(line_split[0]);
            k2.setPrice(Double.parseDouble(line_split[2]));
            context.write(k2, v2);
        }
    }

    public static class GCReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
        @Override
        protected void reduce(OrderBean k2, Iterable<NullWritable> v2s, Context context) throws IOException, InterruptedException {
            context.write(k2, NullWritable.get());
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(Main.class);
            job.setMapperClass(GCMapper.class);
            job.setReducerClass(GCReducer.class);

            job.setMapOutputKeyClass(OrderBean.class);
            job.setMapOutputValueClass(NullWritable.class);
            job.setOutputKeyClass(OrderBean.class);
            job.setOutputValueClass(NullWritable.class);

            //设置reduce端的分组
            job.setGroupingComparatorClass(OrderGroupingComparator.class);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\abuilex\\Documents\\mapreduce\\分组排序问题\\input"));
            Path output = new Path("C:\\Users\\abuilex\\Documents\\mapreduce\\分组排序问题\\output");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("分组排序" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
