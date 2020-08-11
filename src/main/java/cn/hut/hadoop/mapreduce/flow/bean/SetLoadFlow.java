package cn.hut.hdfs.mapreduce.flow.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SetLoadFlow extends FlowBean {
    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeDouble(load);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        load = in.readDouble();
    }
}
