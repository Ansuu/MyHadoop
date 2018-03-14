package org.ansu.testwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
public class Addr implements Writable {
    private Text province;

    public Text getProvince() {
        return province;
    }

    public void setProvince(Text province) {
        this.province = province;
    }

    public void write(DataOutput out) throws IOException {
        province.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        province = new Text();
        province.readFields(in);
    }
}
