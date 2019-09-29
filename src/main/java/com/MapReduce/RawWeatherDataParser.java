package com.MapReduce;

import org.apache.hadoop.io.Text;

/**
 * @program:Hadoop_BD1903
 * @package:com.MapReduce
 * @filename:RawWeatherDataParser.java
 * @create:2019.09.27.16:51:27
 * @auther:李煌民
 * @description:.原始天气数据解析器
 *
 * 0010999999999991992010100004-24000-045700FM-13+9999KNFG V02099999999999999999N9999999N9+99999+99999999999ADDAG12000
 * 0016999999999991992010100004+55700+004700FM-13+9999OU24 V0202301N012319999999N0040001N9+00971+99999102141ADDAG12000MW1101
 * 0024999999999991992010100004+15600+041200FM-13+9999PIGM V0201501N004619999999N0200001N9+02511+02341101231ADDAG12000REMSYN00522234
 * 0036999999999991992010100004+02100-038300FM-13+9999WXVH V0201101N006219999999N0100001N9+99999+99999999999ADDAG12000GF106991999999002501999999
 **/
public class RawWeatherDataParser {
    private int year;
    private String sid;
    private double temp;

    //检测数据是否合法，传递参数为Hadoop的数据类型
    public boolean parse(Text line){
        return this.parse(line.toString());
    }

    //检测数据是否合法，传递参数是java的数据类型
    public boolean parse(String line){
        //数据长度不足
        if (line.length() < 93) return false;

        //未检测到温度
        String str = line.substring(87, 92);
        if ("+9999".equals(str)) return false;

        //数据质量不佳
        if (!"01459".contains(line.substring(92,93))) return false;

        //得到年份
        this.year = Integer.parseInt(line.substring(15, 19));
        //得到气象台编号
        this.sid = line.substring(0, 15);
        //得到温度数据
        this.temp = Double.parseDouble(line.substring(87,92));
        return true;
    }

    public int getYear() {
        return year;
    }

    public String getSid() {
        return sid;
    }

    public double getTemp() {
        return temp;
    }
}
