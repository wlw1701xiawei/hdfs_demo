package cn.hut.hdfs.mapreduce.descindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DescIndex {
    public static class DIMapper extends Mapper<LongWritable, Text, Text, Text> {
        String fileName;
        String[] line_split;
        Text k2 = new Text();
        Text v2 = new Text("" + 1);

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
            line_split = v1.toString().split("[ ,.]");
            for (String word : line_split) {
                if (word != "") {
                    k2.set(fileName + ":" + word);
                    context.write(k2, v2);
                }
            }
        }
    }

    public static class DICombiner extends Reducer<Text, Text, Text, Text> {
        int count = 0;
        int splitIndex;
        String fileName;
        Text k3 = new Text();
        Text v3 = new Text();

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            splitIndex = k2.toString().indexOf(':');
            k3.set(k2.toString().substring(splitIndex + 1));//获取单词
            fileName = k2.toString().substring(0, splitIndex);//获取文件名
            while (v2s.iterator().hasNext()) {
                count ++;
                v2s.iterator().next();
            }
            v3.set(fileName + ":" + count);
            count = 0;//重置
            context.write(k3, v3);
        }
    }

    public static class DIReducer extends Reducer<Text, Text, Text, Text> {
        String fileList = "";
        Text k4;
        Text v4 = new Text();

        @Override
        protected void reduce(Text k3, Iterable<Text> v3s, Context context) throws IOException, InterruptedException {
            k4 = k3;
            while (v3s.iterator().hasNext()) {
                fileList += "\t" + v3s.iterator().next();
            }
            v4.set(fileList);
            fileList = "";//重置
            context.write(k4, v4);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(DescIndex.class);
            job.setMapperClass(DIMapper.class);
            job.setCombinerClass(DICombiner.class);
            job.setReducerClass(DIReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\倒排索引问题\\input"));
            Path output = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\倒排索引问题\\output");
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(output)) {
                fs.delete(output, true);
            }
            FileOutputFormat.setOutputPath(job, output);

            System.out.println("倒排索引生成" + (job.waitForCompletion(true) ? "成功" : "失败"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
