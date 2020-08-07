package com.atguigu.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable,PollableSource{

    //定义全局的前缀&后缀
    private String prefix;
    private String subfix;

    @Override
    public void configure(Context context) {
        //读取配置信息，给前后缀赋值
        //无默认值 对应a1.sources.r1.prefix=feiji
        prefix = context.getString("prefix");
        //有默认值 对应a1.sources.r1.subfix=xiaxian
        subfix = context.getString("subfix","atguigu");
    }

    /**
     * 会不断的执行
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        //1.接收数据
        try {
            //循环造数据（为了不写io流读数据，麻烦）
            for(int i=0;i<5;i++){
                //2.构建事件对象
                SimpleEvent event = new SimpleEvent();
                //3.给事件设置值
                event.setBody((prefix+"--"+i+"--"+subfix).getBytes());
                //4.将事件传给channel
                getChannelProcessor().processEvent(event);
                status = Status.READY;

            }
        } catch (Exception e) {
            e.printStackTrace();
            status =Status.BACKOFF;
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }
}
