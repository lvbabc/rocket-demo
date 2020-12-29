package com.rex.rocket.producer;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class ProducerMessageQueueSelectorDemo {

    public static void main(String[] args) throws MQClientException, InterruptedException, RemotingException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("dw_test_producer_group");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();

        // 订单实体
        Order order = new Order();
        order.setId(1001L);
        order.setOrderNo("2020072823270500001");
        order.setBuyerId(1L);
        order.setSellerId(1L);
        order.setTotalPrice(10000L);
        order.setStatus(0);
        System.out.printf("%s%n", sendMsg(producer, order));

        //订单状态发生变更
        order.setStatus(1);
        //重新发生消息
        System.out.printf("%s%n", sendMsg(producer, order));
        producer.shutdown();
    }

    private static SendResult sendMsg(DefaultMQProducer producer, Order order) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        Message msg = new Message(
                "order_topic",
                null,
                order.getOrderNo(),
                JSON.toJSONString(order).getBytes());

        return producer.send(msg, (mqs, msg1, arg) -> {
            if (mqs == null || mqs.isEmpty()) {
                return null;
            }
            int index = Math.abs(arg.hashCode()) % mqs.size();

            return mqs.get(Math.max(index, 0));
        }, order.getOrderNo());
    }

}

