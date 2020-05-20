package cn.hut.hdfs.mapreduce.srotpriority.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PriorityBean implements WritableComparable<PriorityBean> {
    private String SP1;
    private String SP2;
    private String SP3;
    private String SP4;

    public void setSP1(String SP1) {
        this.SP1 = SP1;
    }

    public void setSP2(String SP2) {
        this.SP2 = SP2;
    }

    public void setSP3(String SP3) {
        this.SP3 = SP3;
    }

    public void setSP4(String SP4) {
        this.SP4 = SP4;
    }

    @Override
    public String toString() {
        return SP1 + "\t" + SP2 + "\t" + SP3 + "\t" + SP4;
    }

    @Override
    public int compareTo(PriorityBean o) {
        //这里我固定优先级SP1>SP2>SP3>SP4
        if (this.SP1.equals(o.SP1)) {
            if (this.SP2.equals(o.SP1)) {
                if (this.SP3.equals(o.SP3)) {
                    return this.SP4.compareTo(o.SP4);
                } else {
                    return this.SP3.compareTo(o.SP3);
                }
            } else {
                return this.SP2.compareTo(o.SP2);
            }
        } else {
            return this.SP1.compareTo(o.SP1);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(SP1);
        out.writeUTF(SP2);
        out.writeUTF(SP3);
        out.writeUTF(SP4);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        SP1 = in.readUTF();
        SP2 = in.readUTF();
        SP3 = in.readUTF();
        SP4 = in.readUTF();
    }
}
