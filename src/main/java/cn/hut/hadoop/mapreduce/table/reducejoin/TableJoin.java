package cn.hut.hadoop.mapreduce.table.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableJoin {
    public static class TJMapper extends Mapper<LongWritable, Text, Text, TableBean> {
        private String fileName;
        private String[] line_split;
        private Text k2 = new Text();
        private TableBean v2;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //获取文件名
            fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            fileName = fileName.substring(0, fileName.indexOf('.'));
        }

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            line_split = v1.toString().split("\t");
            v2 = new TableBean();

            //根据文件名从而用不同的封装方式
            if (fileName.equals("order")) {
                k2.set(line_split[1]);
                v2.setId(line_split[0]);
                v2.setPid(line_split[1]);
                v2.setAmount(Integer.parseInt(line_split[2]));
                v2.setPname("");
            } else if (fileName.equals("pd")) {
                k2.set(line_split[0]);
                v2.setId("");
                v2.setPid(line_split[0]);
                v2.setAmount(0);
                v2.setPname(line_split[1]);
            } else {
                new FileNotFoundException("This file don't exists!");
            }
            v2.setFlag(fileName);

            context.write(k2, v2);
        }
    }

    public static class TJReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {
        private ArrayList<TableBean> orders = new ArrayList<>();
        private String pName;
        private NullWritable v3 = NullWritable.get();

        @Override
        protected void reduce(Text k2, Iterable<TableBean> v2s, Context context) throws IOException, InterruptedException {
            for (TableBean v2 : v2s) {
                if (v2.getFlag().equals("order")) {
                    TableBean tmpBean = new TableBean();
                    try {
                        BeanUtils.copyProperties(tmpBean, v2);
                        orders.add(tmpBean);    //v2是v2s的引用，若添加用的是v2，则v2指向下一个时数组里的内容也变成这下一个
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                } else if (v2.getFlag().equals("pd")) {
                    pName = v2.getPname();
                }
            }
            for (TableBean k3 : orders) {
                k3.setPname(pName);
                context.write(k3, v3);
            }
            orders.clear();
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(TableJoin.class);
            job.setMapperClass(TJMapper.class);
            job.setReducerClass(TJReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(TableBean.class);
            job.setOutputKeyClass(TableBean.class);
            job.setOutputValueClass(NullWritable.class);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\表合并问题\\input"));
            Path output = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\表合并问题\\output");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("表合并" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
