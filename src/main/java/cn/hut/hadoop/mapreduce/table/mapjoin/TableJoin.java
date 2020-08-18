package cn.hut.hadoop.mapreduce.table.mapjoin;

import cn.hut.hadoop.mapreduce.table.reducejoin.TableBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class TableJoin {
    public static class TJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        HashMap<String, String> pdMap = new HashMap<>();
        Text k2 = new Text();
        NullWritable v2 = NullWritable.get();

        @Override
        protected void setup(Context context) throws IOException {
            //缓存小表
            URI[] cacheFiles = context.getCacheFiles();
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFiles[0].getPath().toString()), "UTF-8"));
            String line;
            String[] lineSplit;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                lineSplit = line.split("\t");
                pdMap.put(lineSplit[0], lineSplit[1]);
            }
            IOUtils.closeStream(reader);
        }

        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            String[] lineSplit = v1.toString().split("\t");
            //将pid改为pName
            String line = lineSplit[0] + "\t" +pdMap.get(lineSplit[1]) + "\t" + lineSplit[2];
            k2.set(line);
            context.write(k2, v2);
        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            Job job = Job.getInstance(conf);

            job.setJarByClass(TableJoin.class);
            job.setMapperClass(TJMapper.class);

//            job.setMapOutputKeyClass(Text.class);
//            job.setMapOutputValueClass(TableBean.class);
            job.setOutputKeyClass(TableBean.class);
            job.setOutputValueClass(NullWritable.class);

            //Map端Join的逻辑不需要Reduce阶段，设置reduceTasks为0
            job.setNumReduceTasks(0);
            //加载缓存数据
            job.addCacheFile(new URI("file:///C:/Users/abuilex/Documents/mapreduce/表合并问题/input/pd.txt"));

            FileInputFormat.setInputPaths(job,
                    new Path("C:\\Users\\Abuilex\\Documents\\mapreduce\\表合并问题\\input\\order.txt"));
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
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }
}
