package com.atguigu.mr.flowSum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
    FlowBean v = new FlowBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        // 7 13560436666 120.196.100.99 1116 954 200
        // id 手机号码 网络ip 上行流量 下行流量 网络状态码
        String line = value.toString();
        // 2 切割字段
        String[] fields = line.split("\t");
        // 3 封装对象
        // 取出手机号码
        String phoneNum = fields[1];
        // 取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[fields.length - 3]);//因为原数据中有字段缺失，所以采用这种方式获取
        long downFlow = Long.parseLong(fields[fields.length - 2]);
        k.set(phoneNum);
        v.setDownFlow(downFlow);
        v.setUpFlow(upFlow);
        // 4 写出
        context.write(k, v);
    }
}