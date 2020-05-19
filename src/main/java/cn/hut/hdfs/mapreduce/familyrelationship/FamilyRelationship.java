package cn.hut.hdfs.mapreduce.familyrelationship;

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
import java.util.ArrayList;

public class FamilyRelationship {
    public static class FRMapper extends Mapper<LongWritable, Text, Text, Text> {
        String[] line_split;
        Text k2 = new Text();
        Text v2 = new Text();

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            line_split = v1.toString().split("\t");
            k2.set(line_split[0]);
            //"+"表示此人是k2的子女
            v2.set("+" + line_split[1]);
            context.write(k2, v2);
            k2.set(line_split[1]);
            //"-"表示此人是k2的父母
            v2.set("-" + line_split[0]);
            context.write(k2, v2);
        }
    }

    public static class FRReducer extends Reducer<Text, Text, Text, Text> {
        Text k3 = new Text();
        Text v3 = new Text();
        ArrayList<String> grandparents = new ArrayList<>();
        ArrayList<String> grandchildren = new ArrayList<>();
        String s;

        @Override
        protected void reduce(Text k2, Iterable<Text> v2s, Context context) throws IOException, InterruptedException {
            for (Text v2 : v2s) {
                s = v2.toString();
                if (s.charAt(0) == '-') {
                    grandparents.add(s.substring(1));
                }
                else if (s.charAt(0) == '+') {
                    grandchildren.add(s.substring(1));
                }
            }
            if (!grandparents.isEmpty() && !grandchildren.isEmpty()) {
                for (String i : grandparents) {
                    for (String j : grandchildren) {
                        k3.set(i);
                        v3.set(j);
                        context.write(k3, v3);
                    }
                }
            }
            grandparents.clear();
            grandchildren.clear();
        }

        public static void main(String[] args) {
            Configuration conf = new Configuration();
            try {
                Job job = Job.getInstance(conf);

                job.setJarByClass(FamilyRelationship.class);
                job.setMapperClass(FRMapper.class);
                job.setReducerClass(FRReducer.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.setInputPaths(job, 
                        new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\家庭关系问题\\input"));
                Path output = new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\家庭关系问题\\output");
                FileSystem fs = FileSystem.get(conf);
                if (fs.exists(output)) {
                    fs.delete(output, true);
                }
                FileOutputFormat.setOutputPath(job, output);

                System.out.println("执行" + (job.waitForCompletion(true) ? "成功" : "失败"));
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
