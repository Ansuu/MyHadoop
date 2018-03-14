package org.ansu.testwritable;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Person implements Writable {
    private Text name;
    private IntWritable age;
    private BooleanWritable male;
    private Addr born;
    public Person() {
    }

    public Person(Text name, IntWritable age, BooleanWritable male, Addr born) {
        this.name = name;
        this.age = age;
        this.male = male;
        this.born = born;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }

    public IntWritable getAge() {
        return age;
    }

    public void setAge(IntWritable age) {
        this.age = age;
    }

    public BooleanWritable getMale() {
        return male;
    }

    public void setMale(BooleanWritable male) {
        this.male = male;
    }

    public Addr getBorn() {
        return born;
    }

    public void setBorn(Addr born) {
        this.born = born;
    }

    public void write(DataOutput dataOutput) throws IOException {
        name.write(dataOutput);
        age.write(dataOutput);
        male.write(dataOutput);
        born.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        name = new Text();
        age = new IntWritable();
        male = new BooleanWritable();
        born = new Addr();
        name.readFields(dataInput);
        age.readFields(dataInput);
        male.readFields(dataInput);
        born.readFields(dataInput);
    }
}
