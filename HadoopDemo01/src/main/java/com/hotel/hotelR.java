package com.hotel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author scz
 * @date 2020/10/13
 */
//输入key类型，输入value类型，输出key类型 ，输出value类型
public class hotelR extends Reducer<LongWritable,Text, NullWritable,Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String cn_name = "";
        int comment_num=-1;
        float start, score = -1;
        start = score = -1;

        int start3_num = 0;
        int one_comment = -1;
        int two_comment = -1;
        int three_commnet = -1;
        int four_commnet = -1;
        int five_commnet = -1;

        String one_name = "";
        String two_name = "";
        String three_name = "";
        String four_name = "";
        String five_name = "";

        for (Text val : values) {
            String[] str = val.toString().split(",");
            String[] ac = null;
            for (String a : str) {
                ac = a.split("=");
                if (ac[0].equals("cn_name")) {
                    cn_name = ac[1];
                } else if (ac[0].equals("score")) {
                    score = Float.parseFloat(ac[1]);
                } else if (ac[0].equals("comment_num")) {
                    comment_num = Integer.parseInt(ac[1]);
                } else if (ac[0].equals("start")) {
                    start = Integer.parseInt(ac[1]);
                }
                ac = null;
            }
            //前五
            if(comment_num>one_comment){
                five_commnet=four_commnet;
                four_commnet=three_commnet;
                three_commnet=two_comment;
                two_comment=one_comment;
                one_comment=comment_num;

                five_name=four_name;
                four_name=three_name;
                three_name=two_name;
                two_name=one_name;
                one_name=cn_name;


            }else if(comment_num>two_comment){
                five_commnet=four_commnet;
                four_commnet=three_commnet;
                three_commnet=two_comment;
                two_comment=comment_num;

                five_name=four_name;
                four_name=three_name;
                three_name=two_name;
                two_name=cn_name;
            }else if(comment_num>three_commnet){
                five_commnet=four_commnet;
                four_commnet=three_commnet;
                three_commnet=comment_num;

                five_name=four_name;
                four_name=three_name;
                three_name=cn_name;
            }else if(comment_num>four_commnet){
                five_commnet=four_commnet;
                four_commnet=comment_num;

                five_name=four_name;
                four_name=cn_name;
            }else if(comment_num>five_commnet){
                five_commnet=comment_num;

                five_name=cn_name;
            }
        }

        //三星个数
        if (start == 3) {
            start3_num = start3_num + 1;
        }

        context.write(NullWritable.get(),new Text("三星酒店个数:"+start3_num));
        context.write(NullWritable.get(),new Text("评论最多的酒店："+one_name+",评论数："+one_comment));
        context.write(NullWritable.get(),new Text("评论第二的酒店："+two_name+",评论数:"+two_comment));
        context.write(NullWritable.get(),new Text("评论第三的酒店："+three_name+",评论数:"+three_commnet));
        context.write(NullWritable.get(),new Text("评论第四的酒店："+four_name+",评论数:"+four_commnet));
        context.write(NullWritable.get(),new Text("评论第五的酒店："+five_name+",评论数:"+five_commnet));

    }
}
