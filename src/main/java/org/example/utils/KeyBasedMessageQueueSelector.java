package org.example.utils;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class KeyBasedMessageQueueSelector implements MessageQueueSelector {
    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        String key = (String) arg;
        key = key.substring(key.lastIndexOf("-") + 1);
        int index = Math.abs(key.hashCode()) % mqs.size();
        return mqs.get(index);
    }
}