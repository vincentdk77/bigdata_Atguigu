package com.atguigu.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {
    //创建 Logger 对象
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSink.class);
    private String prefix;
    private String subfix;
    @Override
    public Status process() throws EventDeliveryException {
        //声明返回值状态信息
        Status status= null;
        //获取当前 Sink 绑定的 Channel
        Channel ch = getChannel();
        //获取事务
        Transaction txn = ch.getTransaction();
        //声明事件
        Event event = null;
        // 开启事务
        txn.begin();
        //读取 Channel 中的事件，直到读取到事件结束循环
        while (true) {
            event = ch.take();
            if (event != null) {
                break;
            }
        }
        try {
            //处理事件（打印）
            LOG.info(prefix + new String(event.getBody()) + subfix);
            //事务提交
            txn.commit();
            status = Status.READY;
        } catch (Exception e) {
            //遇到异常，事务回滚
            txn.rollback();
            status = Status.BACKOFF;
        } finally {
            //关闭事务
            txn.close();
        }
        return status;
    }

    @Override
    public void configure(Context context) {
        //无默认值 对应a1.sources.r1.prefix=feiji
        prefix = context.getString("prefix");
        //有默认值 对应a1.sources.r1.subfix=xiaxian
        subfix = context.getString("subfix","atguigu");
    }
}
