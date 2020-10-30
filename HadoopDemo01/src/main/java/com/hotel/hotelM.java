package com.hotel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author scz
 * @date 2020/10/13
 */
//第一个值为行数，第二个值为内容，第三个值为传出key的类型，第四个是传出value的类型
public class hotelM extends Mapper<LongWritable, Text,LongWritable,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String va=value.toString();
        va=va.replaceAll(" ","");

        //星级作为key  计算平均值
        context.write(key,new Text(va));
    }
}
