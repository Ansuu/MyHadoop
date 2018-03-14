package org.ansu.testwritable;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.*;

public class TestPersonSeri {
    private void toseri() throws Exception{
        Person p = new Person();
        p.setAge(new IntWritable(10));
        p.setMale(new BooleanWritable(true));
        p.setName(new Text("Leo"));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        p.write(dos);
        Person np = new Person();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        np.readFields(dis);
        System.out.println(np.getAge().get());
    }
    private void toSeri() throws Exception{
        Person p = new Person();
        Addr addr = new Addr();
        addr.setProvince(new Text("abc"));
        p.setName(new Text("Leo"));
        p.setAge(new IntWritable(10));
        p.setMale(new BooleanWritable(true));
        p.setBorn(addr);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        p.write(dos);
        Person np = new Person();
        np.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
        System.out.println(np.getBorn().getProvince());
    }
    public static void main(String[] args) throws Exception{
        TestPersonSeri testPersonSeri = new TestPersonSeri();
        testPersonSeri.toSeri();
    }
}
