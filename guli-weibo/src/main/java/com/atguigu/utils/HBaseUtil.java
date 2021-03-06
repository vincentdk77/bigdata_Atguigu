package com.atguigu.utils;

import com.atguigu.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;

/**
 * 1.创建命名空间
 * 2.判断表是否存在
 * 3.创建表（三张表）
 */
public class HBaseUtil {

    public static void createNameSpace(String nameSpace) throws IOException {
        //1.获取Connection对象
        Connection  conn= ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //2.获取Admin对象
        Admin admin = conn.getAdmin();
        //3.构建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        //4.创建命名空间
        admin.createNamespace(namespaceDescriptor);
        //5.关闭资源
        admin.close();
        conn.close();
    }

    //2.判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        //1.获取Connection对象
        Connection  conn= ConnectionFactory.createConnection(Constants.CONFIGURATION);
        //2.获取Admin对象
        Admin admin = conn.getAdmin();
        //3.判断admin是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        //4.关闭资源
        admin.close();
        conn.close();
        //返回结果
        return  exists;
    }

    //3.创建表
    public static void createTable(String tableName, int versions, String... cfs) throws IOException {
        //1.判断是否传入了列簇信息
        if(cfs.length<=0){
            System.out.println("请设置列簇信息！");
            return;
        }
        //2.判断表是否存在
        if(isTableExist(tableName)){
            System.out.println(tableName+"表已经存在！");
            return;
        }
        //3.获取connection对象
       Connection conn = ConnectionFactory.createConnection(Constants.CONFIGURATION);

        //4.获取Admin对象
        Admin admin = conn.getAdmin();

        //5.创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //6.循环添加列簇信息
        for(String cf:cfs){
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);

            //7.设置版本 // TODO: 2020/5/16 业务要求,比如要求每个人的微博要显示最新的三条 
            hColumnDescriptor.setMaxVersions(versions);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        //8. 创建表操作
        admin.createTable(hTableDescriptor);
        //9.关闭资源
        admin.close();
        conn.close();
    }
}
