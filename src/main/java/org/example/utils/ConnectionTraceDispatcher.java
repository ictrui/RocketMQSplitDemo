package org.example.utils;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.trace.TraceContext;
import org.apache.rocketmq.client.trace.TraceDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionTraceDispatcher implements TraceDispatcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionTraceDispatcher.class);

    public void append(TraceContext traceContext) {
        if (traceContext.isSuccess()) {
            LOGGER.info("消费者成功连接到 RocketMQ 集群");
        } else {
            LOGGER.error("消费者连接到 RocketMQ 集群失败");
        }
    }

    @Override
    public void start(String nameSrvAddr, AccessChannel accessChannel) throws MQClientException {

    }

    @Override
    public boolean append(Object ctx) {
        return false;
    }

    @Override
    public void flush() {
    }

    @Override
    public void shutdown() {
    }
}
