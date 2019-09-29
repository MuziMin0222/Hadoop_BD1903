package com.Hadoop_IO;

import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

/**
 * @program:Hadoop_BD1903
 * @package:com.Hadoop_IO
 * @filename:CountPatent.java
 * @create:2019.09.25.20:14:16
 * @auther:李煌民
 * @description:.计算专利引用
 *  专利   被引用的专利
 * 3858241,956203
 * 3858241,1324234
 * 3858241,3398406
 * 3858241,3557384
 * 3858241,3634889
 * 3858242,1515701
 * 3858242,3319261
 * 3858242,3668705
 * 3858242,3707004
 * 3858243,2949611
 *    。     。
 *    。     。
 *    。     。
 * 计算每个专利被应用的次数
 * 计算每个专利都引用了哪些专利
 **/
public class CountPatent {

    @Test
    //统计每个专利都引用了哪些专利
    public void CountUsedPatent() throws Exception{
        //将专利文件读取进来
        BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream("D:\\briup培训\\hadoop\\data\\patent\\patent\\cite75_99.txt")));

        HashMap<String, String> map = new HashMap<>();

        String line;
        while ((line = br.readLine()) != null){
            String[] strs = line.split(",");
            String value = map.get(strs[0]);
            if (value == null){
                map.put(strs[0],strs[1] + " ");
            }else {
                value = value + strs[1] + " ";
                map.put(strs[0],value);
            }
        }

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\code\\IODemo\\统计被引用的专利.txt")));

        Set<String> set = map.keySet();
        for (String key : set) {
            bw.write(key + "引用的专利有：" + map.get(key));
            bw.newLine();
            bw.flush();
        }

        bw.close();
        br.close();
    }


    //统计专利引用的次数
    @Test
    public void CountPatent() throws  Exception{
        //将专利文件读取进来
        BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream("D:\\briup培训\\hadoop\\data\\patent\\patent\\cite75_99.txt")));

        //定义一个集合来存储被引用的专利
        HashMap<String, Integer> map = new HashMap<>();

        String line;
        while ((line = br.readLine()) != null){
            String[] strs = line.split(",");
            Integer num = map.get(strs[1]);
            map.put(strs[1],num == null?1:num+1);
        }

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("D:\\code\\IODemo\\专利引用次数.txt")));

        Set<String> set = map.keySet();
        for (String key : set) {
            bw.write(key + "专利引用的次数：" + map.get(key));
            bw.newLine();
            bw.flush();
        }

        bw.close();
        br.close();
    }
}
