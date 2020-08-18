package cn.hut.hadoop.mapreduce.table.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {
    private String id;  //订单id
    private String pid; //产品id
    private int amount; //数量
    private String pname;   //产品名称
    private String flag;    //表标记

    public String getId() {
        return id;
    }

    public String getPid() {
        return pid;
    }

    public int getAmount() {
        return amount;
    }

    public String getPname() {
        return pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        pid = in.readUTF();
        amount = in.readInt();
        pname = in.readUTF();
        flag = in.readUTF();
    }
}
