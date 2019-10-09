package com.MapReduce_Join.DataBaseWriteAndReader;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce_Join.DataBaseWriteAndReader
 * @filename:YearSidTemp.java
 * @create:2019.10.09.19:34:07
 * @auther:李煌民
 * @description:.生成三列数据，并且可以写入和读取数据库中
 **/
public class YearSidTemp implements DBWritable, WritableComparable<YearSidTemp> {
    private IntWritable year;
    private Text sid;
    private DoubleWritable temp;

    public YearSidTemp() {
        this.year = new IntWritable();
        this.sid = new Text();
        this.temp = new DoubleWritable();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        YearSidTemp that = (YearSidTemp) o;
        return Objects.equals(year, that.year) &&
                Objects.equals(sid, that.sid) &&
                Objects.equals(temp, that.temp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(year, sid, temp);
    }

    //比较key值
    @Override
    public int compareTo(YearSidTemp o) {
        int num1 = this.year.compareTo(o.year);
        int num2 = num1==0?this.sid.compareTo(o.sid):num1;
        int num3 = num2==0?this.temp.compareTo(o.temp):num2;
        return num3;
    }

    //序列化到文件中
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.year.write(dataOutput);
        this.sid.write(dataOutput);
        this.temp.write(dataOutput);
    }

    //反序列化到文件中
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.year.readFields(dataInput);
        this.sid.readFields(dataInput);
        this.temp.readFields(dataInput);
    }

    //序列化到数据库中
    @Override
    public void write(PreparedStatement ps) throws SQLException {
        // insert into bd1903.tbl_yst(year,sid,temp)
        // values(?,?,?);
        ps.setInt(1,this.year.get());
        ps.setString(2,this.sid.toString());
        ps.setDouble(3,this.temp.get());
    }

    //从数据库中反序列化到程序中
    @Override
    public void readFields(ResultSet rs) throws SQLException {
        this.year.set(rs.getInt(1));
        this.sid.set(rs.getString(2));
        this.temp.set(rs.getDouble(3));
    }

    public IntWritable getYear() {
        return year;
    }

    public void setYear(IntWritable year) {
        this.year.set(year.get());
    }
    public void setYear(int year) {
        this.year.set(year);
    }

    public Text getSid() {
        return sid;
    }

    public void setSid(Text sid) {
        this.sid.set(sid.toString());
    }
    public void setSid(String sid) {
        this.sid.set(sid);
    }

    public DoubleWritable getTemp() {
        return temp;
    }

    public void setTemp(DoubleWritable temp) {
        this.temp.set(temp.get());
    }
    public void setTemp(double temp) {
        this.temp.set(temp);
    }
}
