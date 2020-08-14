package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient{
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
//        <!-- 参数优先级排序：
//（1）客户端代码中设置的值 >
//（2）ClassPath 下的用户自定义配置 文件 >
//（3）然后是服务器的默认配置
//                -->
        configuration.set("dfs.replication", "2");
        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
        // FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");
        // 2 创建目录
        fs.mkdirs(new Path("/1108/daxian/banzhang"));
        // 3 关闭资源
        fs.close(); }
}