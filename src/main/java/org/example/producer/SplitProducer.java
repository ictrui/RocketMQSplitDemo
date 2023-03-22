package org.example.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.example.utils.ListSplitter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 90930
 * @version 1.0
 * @description TODO
 * @date 2023/3/21 12:53
 */
public class SplitProducer {
    public static void main(String[] args) throws Exception {
        //实例化消息生产者对象
        DefaultMQProducer producer = new DefaultMQProducer("group_luke");
        //设置NameSever地址
        producer.setNamesrvAddr("172.16.2.74:9876");
        producer.setSendMsgTimeout(10000); // 设置为 10 秒

        //启动Producer实例
        producer.start();

        String topic = "topic_luke";

        List<Message> messageList = new ArrayList<>();

        for (int i = 0; i < 1000 * 100; i++) {
            messageList.add(new Message(topic,"tag","keys"+i,("消息体"+i).getBytes(StandardCharsets.UTF_8)));
        }
        //分割
        ListSplitter splitter = new ListSplitter(messageList);
        while (splitter.hasNext()){
            List<Message> listItem = splitter.next();
            producer.send(listItem);
        }
        //关闭producer
        producer.shutdown();
    }
}
