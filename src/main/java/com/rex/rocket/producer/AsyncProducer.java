package com.rex.rocket.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

public class AsyncProducer {
    public static void main(String[] args) throws InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("testProducerGroup");

        producer.setNamesrvAddr("127.0.0.1:9876");

        try {
            producer.start();
            //发送单条消息
            Message msg = new Message("TOPIC_TEST", "hello rocketmq".getBytes());
            producer.send(msg, new SendCallback() {
                // 消息发送成功回调函数
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.printf("%s%n", sendResult);
                }

                //　消息发送失败回调函数
                @Override
                public void onException(Throwable e) {
                    e.printStackTrace();
                    //　消息发送失败，可以在这里做补偿，例如将消息存储到数据库，定时重试。
                }
            });
        } catch (MQClientException | RemotingException | InterruptedException e) {
            e.printStackTrace();
            //消息发送失败，可以在这里做补偿，例如将消息存储到数据库，定时重试。
        }
        TimeUnit.SECONDS.sleep(3);

        //　使用完毕后，关闭消息发送者
        // 基于 Spring Boot 的应用，在消息发送的时候并不会调用 shutdown 方法，而是等到 spring 容器停止
        producer.shutdown();
    }
}
