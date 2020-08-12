package cn.hut.hadoop.mapreduce.srotpriority;

import cn.hut.hadoop.mapreduce.srotpriority.bean.PriorityBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SPPartitioner extends Partitioner<PriorityBean, NullWritable> {
    @Override
    public int getPartition(PriorityBean k, NullWritable v, int numPartitions) {
        if (k.getSP1().matches("[a-m]")) { //equals()比较值，而matches()可用于匹配正则表达式
            return 1;
        } else if (k.getSP1().matches("[n-z]")) {
            return 2;
        } else {
            return 0;
        }
    }
}
