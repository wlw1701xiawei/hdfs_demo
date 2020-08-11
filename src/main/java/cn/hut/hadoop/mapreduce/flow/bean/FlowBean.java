package cn.hut.hdfs.mapreduce.flow.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;

public class FlowBean implements WritableComparable<FlowBean> {
    protected Double upload = 0.0;
    protected Double download = 0.0;
    protected Double load;

    public FlowBean() {}

    public Double getUpload() {
        return upload;
    }

    public FlowBean setUpload(Double upload) {
        this.upload = upload;
        return this;
    }

    public Double getDownload() {
        return download;
    }

    public FlowBean setDownload(Double download) {
        this.download = download;
        return this;
    }

    public void setLoad() {
        load = new BigDecimal(upload.toString()).add(new BigDecimal(download.toString())).doubleValue();
    }

    public void setLoad(Double load) {
        this.load = load;
    }

    public FlowBean addFlowBean(FlowBean other) {
        return this.setUpload(
                new BigDecimal(this.getUpload().toString()).add(new BigDecimal(other.getUpload().toString())).doubleValue()
        ).setDownload(
                new BigDecimal(this.getDownload() .toString()).add(new BigDecimal(other.getDownload().toString())).doubleValue()
        );
    }

    @Override
    public String toString() {
        return  upload + "\t" + download + "\t" + load;
    }

    @Override
    public int compareTo(FlowBean o) {
        //降序排序关键
        return Double.compare(o.load, this.load);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(upload);
        out.writeDouble(download);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upload = in.readDouble();
        download = in.readDouble();
    }
}
