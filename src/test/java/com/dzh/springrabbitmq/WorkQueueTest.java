package com.dzh.springrabbitmq;

import com.dzh.springrabbitmq.utils.RabbitMQ;
import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 一个生产者
 * 一个默认交换机
 * 一个队列
 * 多个消费者 共同消费这个队列的消息
 */
@SpringBootTest
public class WorkQueueTest {

    public static final String QUEUE_NAME = "work";

    @Test
    void publish() throws IOException, TimeoutException {
        Connection connection = RabbitMQ.getConnection();
        Channel channel = connection.createChannel();
        for (int i = 0; i < 100; i++) {
            channel.basicPublish("", QUEUE_NAME,null,("消息"+i).getBytes());

        }

        System.out.println("消息发送完成");

        //关闭资源
        channel.close();
        connection.close();
    }

    /**
     * 消费者 1 号
     */

    @Test
    void consumer01() throws IOException, TimeoutException {
        Connection connection = RabbitMQ.getConnection();
        Channel channel = connection.createChannel();

        //绑定队列
        channel.queueDeclare(QUEUE_NAME,true,false,false,null);

//        指定消费者每次消费几个消息
        channel.basicQos(1);

        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            //2s 消费一个
                try {
                    TimeUnit.SECONDS.sleep(2);
//                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                String msg = new String(body, StandardCharsets.UTF_8);
                System.out.println("内容02:" + msg);

                //消费完成后手动确认已消费
                channel.basicAck(envelope.getDeliveryTag(),false);
            }
        };


        //第二个参数为 false 关闭自动 ACK
        channel.basicConsume(QUEUE_NAME,false,consumer);
        System.in.read();
        channel.close();
        connection.close();

    }

    /**
     * 消费者 2 号
     */

    @Test
    void consumer02() throws IOException, TimeoutException {
        Connection connection = RabbitMQ.getConnection();
        Channel channel = connection.createChannel();

        //绑定队列
        channel.queueDeclare(QUEUE_NAME,true,false,false,null);

//        指定消费者每次消费几个消息
        channel.basicQos(1);

        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
            //1s 消费一个
                try {
                    TimeUnit.SECONDS.sleep(1);
//                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                String msg = new String(body, StandardCharsets.UTF_8);
                System.out.println("内容01:" + msg);

                //消费完成后手动确认已消费
                channel.basicAck(envelope.getDeliveryTag(),false);
            }
        };


        //第二个参数为 false 关闭自动 ACK
        channel.basicConsume(QUEUE_NAME,false,consumer);
        System.in.read();
        channel.close();
        connection.close();

    }
}

