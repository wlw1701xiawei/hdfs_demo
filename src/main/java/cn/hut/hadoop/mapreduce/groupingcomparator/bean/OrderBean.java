package cn.hut.hadoop.mapreduce.groupingcomparator.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Order implements WritableComparable<Order> {
    private String id;
    private Double price;

    public String getId() {
        return id;
    }

    public Double getPrice() {
        return price;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return  id + "\t" + price;
    }

    @Override
    public int compareTo(Order o) {
        //订单id升序，交易额降序
        return this.id.equals(o.id) ? - this.price.compareTo(o.price) : this.id.compareTo(o.id);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        price = in.readDouble();
    }
}
