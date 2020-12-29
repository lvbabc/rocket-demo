package com.rex.rocket.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

public class ProducerDemo {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        // 发送单条消息
        SendResult sendResult = sendOneMsg(producer);
        // 输出结果
        System.out.printf("%s%n", sendResult);

        // 发送带 Key 的消息
        sendResult = sendMsgWithKey(producer);
        // 输出结果
        System.out.printf("%s%n", sendResult);

        // 批量发送
        sendResult = sendMsgBatch(producer);
        System.out.printf("%s%n", sendResult);

        producer.shutdown();
    }

    private static SendResult sendOneMsg(DefaultMQProducer producer) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        // 发送单条消息
        Message msg = new Message("TOPIC_TEST", "hello rocketmq".getBytes());
        return producer.send(msg);
    }

    private static SendResult sendMsgWithKey(DefaultMQProducer producer) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        // 发送带 Key 的消息
        Message msg = new Message(
                "TOPIC_TEST",
                null,
                "ODS2020072615490001",
                "{\"id\":1, \"orderNo\":\"ODS2020072615490001\",\"buyerId\":1,\"sellerId\":1 }".getBytes());
        return producer.send(msg);
    }

    private static SendResult sendMsgBatch(DefaultMQProducer producer) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        // 批量发送
        List<Message> msgs = new ArrayList<>();
        msgs.add(new Message(
                "TOPIC_TEST",
                null,
                "ODS2020072615490002",
                "{\"id\":2, \"orderNo\":\"ODS2020072615490002\",\"buyerId\":1,\"sellerId\":3  }".getBytes()));
        msgs.add(new Message(
                "TOPIC_TEST",
                null,
                "ODS2020072615490003",
                "{\"id\":4, \"orderNo\":\"ODS2020072615490003\",\"buyerId\":2,\"sellerId\":4  }".getBytes()));
        return producer.send(msgs);
    }
}
