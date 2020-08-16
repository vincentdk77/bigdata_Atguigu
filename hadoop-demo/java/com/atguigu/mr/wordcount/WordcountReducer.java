package com.atguigu.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

// KEYIN, VALUEIN   map阶段输出的key和value
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	IntWritable v = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		
//		atguigu,1
//		atguigu,1
		int sum = 0;
		
		// 1 累加求和
		for (IntWritable value : values) {
			
			sum += value.get();
		}

		v.set(sum);
		
		// 2 写出 atguigu 2
		context.write(key, v);
	}
}
		