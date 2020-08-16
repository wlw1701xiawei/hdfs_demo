package cn.hut.hadoop.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;

public class WebFilterRecordWriter extends RecordWriter<Text, NullWritable> {
    FSDataOutputStream fosFav, fosOther;
    ArrayList<String> favWeb = new ArrayList<>();
    String keyString;

    public WebFilterRecordWriter(TaskAttemptContext job) {
        favWeb.add("https://www.github.com/wlw1701xiawei");
        favWeb.add("https://www.hut.com");
        try {
            //获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());
            //创建两个输出流
            fosFav = fs.create(new Path("C:\\Users\\abuilex\\Documents\\mapreduce\\自定义OutputFormat\\output\\FavWeb.log"));
            fosOther = fs.create(new Path("C:\\Users\\abuilex\\Documents\\mapreduce\\自定义OutputFormat\\output\\OtherWeb.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        //判断key中是否有我收藏的网址，有则写出到FavWeb.log，不是则写入到OtherWeb.log
        keyString = key.toString();
        if (favWeb.contains(keyString)) {
            fosFav.write((keyString + "\r\n").getBytes());
        } else {
            fosOther.write((keyString + "\r\n").getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(fosFav);
        IOUtils.closeStream(fosOther);
    }
}
