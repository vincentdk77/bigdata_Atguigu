package com.atguigu.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义 InputFormat 流程
 * （1）自定义一个类继承 FileInputFormat
 * （2）改写 RecordReader，实现一次读取一个完整文件封装为 KV
 */
public class WholeFileInputformat extends FileInputFormat<Text, BytesWritable>{

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		
		WholeRecordReader recordReader = new WholeRecordReader();
		recordReader.initialize(split, context);
		
		return recordReader;
	}

}
