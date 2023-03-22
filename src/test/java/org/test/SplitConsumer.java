package org.test;

import lombok.SneakyThrows;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author 90930
 * @version 1.0
 * @description TODO
 * @date 2023/3/21 12:55
 */
public class SplitConsumer {
    public static void main(String[] args) throws Exception {
        //实例化消息消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("luke_group_order");
        //指定nameserver地址
        consumer.setNamesrvAddr("172.16.2.74:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        //订阅topic
        consumer.subscribe("topic_luke","*");
        // 注册回调实现类来处理从broker拉取回来的消息
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @SneakyThrows
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                context.setAutoCommit(true);
                for (MessageExt msg : msgs) {
                    System.out.println(Thread.currentThread().getName()+"-"+new String(msg.getBody()));
                }

                try {
                    //模拟业务逻辑处理
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    //报错之后，等一会再处理这批消息，而不是放入重试队列。
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
                // 标记该消息已经被成功消费
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        // 启动消费者实例
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
