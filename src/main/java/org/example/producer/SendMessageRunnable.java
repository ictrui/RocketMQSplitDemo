package org.example.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.example.utils.KeyBasedMessageQueueSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class SendMessageRunnable implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(SendMessageRunnable.class);
    private DefaultMQProducer producer;
    private Message message;
    private KeyBasedMessageQueueSelector selector;
    private String keys;

    public SendMessageRunnable(DefaultMQProducer producer, Message message, KeyBasedMessageQueueSelector selector, String keys) {
        this.producer = producer;
        this.message = message;
        this.selector = selector;
        this.keys = keys;
    }

    @Override
    public void run() {
        try {
            SendResult sendResult = producer.send(message, selector, keys);
            logger.info("Thread: {}, SendResult status:{}, queueId:{}, body:{}",
                    Thread.currentThread().getName(),
                    sendResult.getSendStatus(),
                    sendResult.getMessageQueue().getQueueId()
            );
        } catch (Exception e) {
            logger.error("Error sending message", e);
        }
    }
}