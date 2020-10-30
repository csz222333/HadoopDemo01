package com.hotel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author scz
 * @date 2020/10/13
 */
public class hotelJ {
    public static void main(String[] args) throws  Exception{
        Configuration cof=new Configuration();
        Job job=Job.getInstance(cof);

        //设置三个类
        job.setMapperClass(hotelM.class);
        job.setReducerClass(hotelR.class);
        job.setJarByClass(hotelJ.class);


        //设置输出输入key和value
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        Path path=new Path("hdfs://master:9000/outputdata");
        FileSystem fs=path.getFileSystem(cof);

        if (fs.exists(path)){
            fs.delete(path);
        }

        FileInputFormat.setInputPaths(job,new Path("hdfs://master:9000/inputdata"));
        FileOutputFormat.setOutputPath(job,path);

        Boolean flag=job.waitForCompletion(true);

        if(flag){
            System.out.println("yes");
        }else {
            System.out.println("no");
        }
    }
}
