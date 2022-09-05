package com.dzh.springrabbitmq;

import com.dzh.springrabbitmq.utils.RabbitMQ;
import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 * 广播模式[发布订阅模式] 多个消费者都能收到
 */
public class PubSubTest {

    //交换机名称
    public static final String EXCHANGE_NAME = "pubsub-exchange";

    @Test
    void publish() throws IOException, TimeoutException {
        String msg = "hello";
        Connection connection = RabbitMQ.getConnection();
        Channel channel = connection.createChannel();

        //定义[声明]交换机 FANOUT:广播模式
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);


        //发布消息
        channel.basicPublish(EXCHANGE_NAME,"",null,msg.getBytes());

        //关闭连接
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

        //创建交换机绑定队列
        //定义交换机
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        //定义临时队列   队列名称：queueName
        String queueName = channel.queueDeclare().getQueue();

        //绑定交换机
        channel.queueBind(queueName,EXCHANGE_NAME,"");

//        指定消费者每次消费几个消息
        channel.basicQos(1);

        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    //500ms

                    TimeUnit.MILLISECONDS.sleep(500);
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
        channel.basicConsume(queueName,false,consumer);
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

        //创建交换机绑定队列
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        //绑定交换机
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName,EXCHANGE_NAME,"");

//        指定消费者每次消费几个消息
        channel.basicQos(1);

        DefaultConsumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    //500ms
                    TimeUnit.MILLISECONDS.sleep(500);

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
        channel.basicConsume(queueName,false,consumer);
        System.in.read();
        channel.close();
        connection.close();

    }
}
