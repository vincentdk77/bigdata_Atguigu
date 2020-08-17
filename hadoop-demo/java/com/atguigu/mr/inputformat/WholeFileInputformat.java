package com.atguigu.mr.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * 自定义 InputFormat 流程
 * （1）自定义一个类继承 FileInputFormat
 * （2）改写 RecordReader，实现一次读取一个完整文件封装为 KV
 * <p>
 *
 * 需求：
 * 将多个小文件合并成一个 SequenceFile 文件（SequenceFile 文件是 Hadoop 用来存储二 进制形式的 key-value 对的文件格式），
 * SequenceFile 里面存储着多个文件，存储的形式为 文件路径+名称为 key，文件内容为 value。
 */
public class WholeFileInputformat extends FileInputFormat<Text, BytesWritable> {

	/**
	 * 返回false表示不可切割（分片）
	 * @param context
	 * @param filename
	 * @return
	 */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

	/**
	 * 创建自定义RecordReader对象，并初始化
	 * @param split
	 * @param context
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(split, context);

        return recordReader;
    }

}
