package com.MapReduce_Join.reduce_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Join.reduce_join
 * @filename:KeyFlag.java
 * @create:2019.10.09.17:05:27
 * @auther:李煌民
 * @description:.给key增加一个标记
 **/
public class KeyFlag implements WritableComparable<KeyFlag> {
    private Text key;
    private Text flag;

    public KeyFlag() {
        this.key = new Text();
        this.flag = new Text();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyFlag keyFlag = (KeyFlag) o;
        return Objects.equals(key, keyFlag.key) &&
                Objects.equals(flag, keyFlag.flag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, flag);
    }

    //key值的比较
    @Override
    public int compareTo(KeyFlag o) {
        int num1 = this.key.compareTo(o.key);
        int num2 = num1==0?this.flag.compareTo(o.flag):num1;
        return num2;
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.key.write(dataOutput);
        this.flag.write(dataOutput);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.key.readFields(dataInput);
        this.flag.readFields(dataInput);
    }

    public Text getKey() {
        return key;
    }

    public void setKey(Text key) {
        this.key.set(key.toString());
    }

    public Text getFlag() {
        return flag;
    }

    public void setFlag(Text flag) {
        this.flag.set(flag.toString());
    }
    public void setFlag(String flag) {
        this.flag.set(flag);
    }

    @Override
    public String toString() {
        return "KeyFlag{" +
                "key=" + key +
                ", flag=" + flag +
                '}';
    }
}
