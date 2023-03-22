package org.example;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.example.utils.DatabaseUtils;
import org.example.utils.ExportToTextFile;
import org.example.utils.FileUtils;
import org.example.utils.MessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.example.utils.ExportToTextFile.writeStringToFile;

public class RocketMQConsumer {

    private String consumerGroup;
    private String namesrvAddr;
    private String topic;
    private String tags;
    private String outputFilePath;

    public RocketMQConsumer(String consumerGroup, String namesrvAddr, String topic, String tags, String outputFilePath) {
        this.consumerGroup = consumerGroup;
        this.namesrvAddr = namesrvAddr;
        this.topic = topic;
        this.tags = tags;
        this.outputFilePath = outputFilePath;
    }
    private static String charsetName = "UTF-8";
    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQConsumer.class);
    private static AtomicBoolean isConnected = new AtomicBoolean(false);

    public static void main(String[] args) throws Exception {
        RocketMQConsumer consumer = new RocketMQConsumer("consumerGroup", "172.16.2.74:9876", "DemoTopic2", "*", "src" +
                "/main/resources/output");
        consumer.start();
    }
    public void start() throws Exception {

//        init();
        // 创建一个消费者组
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);

        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        consumer.setNamesrvAddr(namesrvAddr);

        consumer.subscribe(topic, tags);

        consumer.setMessageModel(MessageModel.BROADCASTING);

        // 注册消息监听器
        // 使用 Map 存储分片
        Map<String, List<Message>> messageChunksMap = new ConcurrentHashMap<>();

        AtomicBoolean isConnected = new AtomicBoolean(false);


// 在消息监听器中处理分片
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                if (isConnected.compareAndSet(false, true)) {
                    LOGGER.info("消费者成功连接到 RocketMQ 集群");
                }
                for (MessageExt msg : msgs) {
                    String messageId = msg.getKeys().substring(msg.getKeys().lastIndexOf("-")+1);
                    int chunkIndex = Integer.parseInt(msg.getUserProperty("chunkIndex"));
                    int numOfChunks = Integer.parseInt(msg.getUserProperty("numOfChunks"));
                    LOGGER.info("收到消息：{}，chunkIndex：{}，chunkCount：{}", messageId, chunkIndex, numOfChunks);
                    // 根据 msgId 将消息添加到 messageChunksMap
                    messageChunksMap.putIfAbsent(messageId, new ArrayList<>(Collections.nCopies(numOfChunks, null)));
                    List<Message> messageChunks = messageChunksMap.get(messageId);
                    // 检查分片是否已经收到，避免重复分片
                    if (messageChunks.get(chunkIndex) == null) {
                        messageChunks.set(chunkIndex, msg);
                    }

                    // 检查是否收到了所有分片
                    boolean isComplete = messageChunks.stream().allMatch(Objects::nonNull);
                    if (isComplete) {
                        messageChunks.sort(Comparator.comparingInt(MessageUtil::getOrder));
                        String messageBody;
                        // 组装大消息
                        Message assembledMessage = MessageUtil.assembleMessage(messageChunks);
                        try {
                            messageBody = new String(assembledMessage.getBody(), charsetName);
                        } catch (UnsupportedEncodingException e) {
                            throw new RuntimeException(e);
                        }
                        LOGGER.info("消费大消息：msgId={}, body={}", messageId, messageBody.substring(0,
                                Math.min(messageBody.length(), 1000)));
                        //  resource关系
                        outputFilePath = outputFilePath + "/" + messageId + ".txt";
                        FileUtils.outputFile(messageBody,outputFilePath);
                        // 移除已处理的消息分片
                        messageChunksMap.remove(messageId);
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });


        // 启动消费者
        consumer.start();
        LOGGER.info("Consumer started.");
    }



//    private static void init() {
//        String sql = "SELECT * FROM test_innodb";
//        try {
//            URL resourceUrl = ExportToTextFile.class.getClassLoader().getResource("");
//            String resourcesPath = resourceUrl.getPath();
//            String outputFilePath = resourcesPath + "input.txt";
//            String resultSetString = DatabaseUtils.executeQuery(sql,"jdbc:mysql://172.16.2.74:3306/helloJiu","root",
//                    "QH)qb}GR7EA*");
//            writeStringToFile(resultSetString, outputFilePath);
//        } catch (Exception e) {
//            LOGGER.info("Error executing query and exporting to text file: " + e.getMessage(), e);
//        }
//    }
}
