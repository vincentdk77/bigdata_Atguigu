package com.atguigu.mr.order;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * 订单 id 商品 id 成交金额
 * 0000001 Pdt_01 222.8
 * 		   Pdt_02 33.8
 * 0000002 Pdt_03 522.8
 * 		   Pdt_04 122.4
 * 		   Pdt_05 722.4
 * 0000003 Pdt_06 232.8
 * 		   Pdt_02 33.8
 * 需求：
 * 求出每一个订单中最贵的商品
 *
 * 期望输出数据
 * 1 222.8
 * 2 722.4
 * 3 232.8
 *
 * 分析：
 * （1）利用“订单 id 和成交金额”作为 key，可以将 Map 阶段读取到的所有订单数据按 照 id 升序排序，如果 id 相同再按照金额降序排序，发送到 Reduce。
 * （2）在 Reduce 端利用 groupingComparator 将订单 id 相同的 kv 聚合成组，然后取第一 个即是该订单中最贵商品
 */
public class OrderDriver {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		// 输入输出路径需要根据自己电脑上实际的输入输出路径设置
		args = new String[] { "e:/input/inputorder", "e:/output5" };

		// 1 获取配置信息
		Configuration conf = new Configuration();	
		Job job = Job.getInstance(conf);

		// 2 设置jar包加载路径
		job.setJarByClass(OrderDriver.class);

		// 3 加载map/reduce类
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);

		// 4 设置map输出数据key和value类型
		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);

		// 5 设置最终输出数据的key和value类型
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		// 6 设置输入数据和输出数据路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 8 设置reduce端的分组 // TODO: 2020/8/17
		job.setGroupingComparatorClass(OrderGroupingComparator.class);

		// 7 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}
