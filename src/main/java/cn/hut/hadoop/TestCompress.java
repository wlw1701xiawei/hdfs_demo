package cn.hut.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class TestCompress {
    public static void main(String[] args) {
        //压缩
        compress("C:\\Users\\abuilex\\Documents\\mapreduce\\单词计数问题\\input\\file2.txt",
                "org.apache.hadoop.io.compress.BZip2Codec");
        //解压缩
        decompress("C:\\Users\\abuilex\\Documents\\mapreduce\\单词计数问题\\input\\file2.txt.bz2");
    }

    private static void compress(String fileName, String method) {
        try {
            //获取输入流
            FileInputStream fis = new FileInputStream(new File(fileName));
            //获取输出流
            CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(Class.forName(method), new Configuration());
            FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
            CompressionOutputStream cos = codec.createOutputStream(fos);
            //流的对拷
            IOUtils.copyBytes(fis, cos, 1024*1024, false);
            //关闭资源
            IOUtils.closeStream(cos);
            IOUtils.closeStream(fos);
            IOUtils.closeStream(fis);
        } catch (FileNotFoundException | ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void decompress(String fileName) {
        //检查压缩方式
        CompressionCodec codec = new CompressionCodecFactory(new Configuration()).getCodec(new Path(fileName));
        if (codec == null) {
            new Exception("Not found this codec!");
        }
        try {
            //获取输入流
            FileInputStream fis = new FileInputStream(new File(fileName));
            CompressionInputStream cis = codec.createInputStream(fis);
            //获取输出流
            FileOutputStream fos = new FileOutputStream(new File(fileName + ".decodec"));
            //流的对拷
            IOUtils.copyBytes(cis, fos, 1024*1024, false);
            //关闭资源
            IOUtils.closeStream(fos);
            IOUtils.closeStream(cis);
            IOUtils.closeStream(fis);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
