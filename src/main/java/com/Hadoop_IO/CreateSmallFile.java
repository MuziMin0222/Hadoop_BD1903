package com.Hadoop_IO;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

/**
 * @program:Hadoop_BD1903
 * @package:com.Hadoop_IO
 * @filename:CreateSmallFile.java
 * @create:2019.09.26.19:33:01
 * @auther:李煌民
 * @description:.创建一千个小文件
 **/
public class CreateSmallFile {
    public static void main(String[] args) throws FileNotFoundException {
        File dir = new File("D:\\code\\IODemo\\SmallFile");
        //如果该目录不存在，就创建，包括父目录
        if (!dir.exists()){
            dir.mkdirs();
        }

        for (int i = 0; i < 100000; i++) {
            File file = new File(dir, i + ".txt");
            PrintWriter pw = new PrintWriter(new FileOutputStream(file));
            pw.println("李煌民" + i);
            pw.close();
        }
    }
}
