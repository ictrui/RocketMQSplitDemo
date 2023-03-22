package org.example;

import com.google.common.base.Stopwatch;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.example.consumer.CustomMessageIdGenerator;
import org.example.producer.SendMessageRunnable;
import org.example.utils.FileUtils;
import org.example.utils.KeyBasedMessageQueueSelector;
import org.example.utils.MessageUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RocketMQProducer {

    private static final Logger logger = LoggerFactory.getLogger(RocketMQProducer.class);
    private String producerGroup;
    private String namesrvAddr;
    private String inputFilePath;


    private String demoTopic;
    private String demoTag;

    public RocketMQProducer(String producerGroup, String namesrvAddr, String inputFilePath, String demoTopic,
                            String demoTag) {
        this.producerGroup = producerGroup;
        this.namesrvAddr = namesrvAddr;
        this.inputFilePath = inputFilePath;
        this.demoTopic = demoTopic;
        this.demoTag = demoTag;
    }

    public static void main(String[] args) throws Exception {
        RocketMQProducer producer = new RocketMQProducer("producerGroup", "172.16.2.74:9876", "input.txt", "DemoTopic2", "DemoTag");
        producer.start();
    }

    public void start() throws Exception {
        Stopwatch stopwatch = Stopwatch.createStarted();

        try {
            KeyBasedMessageQueueSelector selector = new KeyBasedMessageQueueSelector();
            DefaultMQProducer producer = new DefaultMQProducer(producerGroup);

            String keys = CustomMessageIdGenerator.generateMessageId();
            producer.setNamesrvAddr(namesrvAddr);
            producer.start();

            String largeString = FileUtils.readFromFile(inputFilePath);

            if (largeString == null) {
                int size = 10 * 1024 * 1024;
                largeString = createLargeString(size);
            }

            byte[] largebytes = largeString.getBytes("UTF-8");
            double sizeInMB = (double) largebytes.length / (1024 * 1024);
            /* 文件有多大*/
            logger.info("\"largebytes size: \"  {} \" MB\"", sizeInMB);

            Message largeMessage = new Message(demoTopic, demoTag, keys, largebytes);

            int chunkSize = 4 * 1024 * 1024;
            List<Message> messageChunks = MessageUtil.splitMessage(largeMessage, chunkSize, keys);

            producer.setSendMsgTimeout(100000);

            int numberOfThreads = 1;
            ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

            for (Message chunk : messageChunks) {
                SendMessageRunnable sendMessageRunnable = new SendMessageRunnable(producer, chunk, selector, keys);
                executorService.submit(sendMessageRunnable);
            }

            executorService.shutdown();
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            producer.shutdown();
        } catch (Exception e) {
            logger.error("Producer encountered an error.", e);
        }
        stopwatch.stop();
        System.out.printf("执行时长：%d 秒. %n", stopwatch.elapsed().getSeconds());
        System.out.printf("执行时长：%d 豪秒.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static String createLargeString(int size) {
        char[] largeCharArray = new char[size];
        Arrays.fill(largeCharArray, 'A');
        return new String(largeCharArray);
    }
}
