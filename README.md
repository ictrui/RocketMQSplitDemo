# rocketmq分割大消息后拼接

```
测试rocketmq 传输sql 查询结果可用性
```



### Usage example 使用示例

1. 读取数据库文件到**input.txt**



   进入`org/example/producer/ExportToTextFile.java` 修改数据库配置后执行输出
2. **开启生产者**



   启动`org/example/RocketMQConsumer.java`,修改其main函数配置

   ```java
   RocketMQConsumer consumer = new RocketMQConsumer("consumerGroup", "172.16.2.74:9876", "DemoTopic2", "*", "src" +
                   "/main/resources/output");
   consumer.start();
   ```
3. **开启消费者**

   


   启动`org/example/RocketMQProducer.java`,修改其main函数配置,读取数据

   ```java
   RocketMQProducer producer = new RocketMQProducer("producerGroup", "172.16.2.74:9876", "input.txt", "DemoTopic2", "DemoTag");
   producer.start();
   ```
4. 进入`src/main/resources/output`,查看输出内容，消费者成功将消息拼接输出至文件，

## 测试



测试文件大小：17M

##### 多线程生产者 (Thread = 4)：

```
时间:11s
```


![image-20230322201617501](http://typora-pc.oss-cn-hangzhou.aliyuncs.com/img/image-20230322201617501.png)



**单线程生产者** ：

     时间:   13724ms
![image-20230322201806008](http://typora-pc.oss-cn-hangzhou.aliyuncs.com/img/image-20230322201806008.png)
