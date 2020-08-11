package cn.hut.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {
    FileSystem fs;

    public HdfsClient() {
        Configuration conf = new Configuration();
        String user = "abuilex";
        try {
            fs = FileSystem.get(new URI("hdfs://master:9000"), conf, user);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void mkDir(String newPath) {
        try {
            fs.mkdirs(new Path(newPath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void rmDirOrFile(Path path) {
        try {
            fs.delete(path, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isExists(Path path) {
        try {
            return fs.exists(path);
        } catch (IOException e) {
            e.printStackTrace();
            return true;
        }
    }

    public void copyToLocalFile(String fromPath, String toPath) {
        Path src = new Path(fromPath);
        Path dst = new Path(toPath);
        try {
            fs.copyToLocalFile(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void copyFromLocalFile(String fromPath, String toPath) {
        Path src = new Path(fromPath);
        Path dst = new Path(toPath);
        try {
            fs.copyFromLocalFile(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void moveToLocalFile(String fromPath, String toPath) {
        Path src = new Path(fromPath);
        Path dst = new Path(toPath);
        try {
            fs.moveToLocalFile(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void moveFromLocalFile(String fromPath, String toPath) {
        Path src = new Path(fromPath);
        Path dst = new Path(toPath);
        try {
            fs.moveFromLocalFile(src, dst);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    void close() {
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
