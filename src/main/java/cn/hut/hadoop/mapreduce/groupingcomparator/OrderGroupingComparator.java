package cn.hut.hadoop.mapreduce.groupingcomparator;

import cn.hut.hadoop.mapreduce.groupingcomparator.bean.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {
    public OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        //只要id相同，就当是相同的key
        return ((OrderBean)a).getId().compareTo(((OrderBean)b).getId());
    }
}
