package com.dzh.springrabbitmq;

import com.dzh.springrabbitmq.utils.RabbitMQ;
import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 路由模式
 */
public class RouteTest {

    public static final String ROUTE_EXCHANGE = "route-exchange";
    public static final String ERROR = "error";
    public static final String INFO = "info";
    public static final String WARNING = "warning";

    @Test
    void publish() throws IOException, TimeoutException {
        String msg = "hello";
        Connection connection = RabbitMQ.getConnection();
        Channel channel = connection.createChannel();

        //定义[声明]交换机 DIRECT:路由模式
        channel.exchangeDeclare(ROUTE_EXCHANGE, BuiltinExchangeType.DIRECT);


        //发布消息
        channel.basicPublish(ROUTE_EXCHANGE, ERROR,null,"错误消息01".getBytes());
        channel.basicPublish(ROUTE_EXCHANGE, INFO,null,"提示消息".getBytes());
        channel.basicPublish(ROUTE_EXCHANGE, ERROR,null,"错误消息02".getBytes());
        channel.basicPublish(ROUTE_EXCHANGE, WARNING,null,"警告消息".getBytes());

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
        channel.exchangeDeclare(ROUTE_EXCHANGE, BuiltinExchangeType.DIRECT);

        //定义临时队列   队列名称：queueName
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName,ROUTE_EXCHANGE,ERROR);

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
        channel.exchangeDeclare(ROUTE_EXCHANGE, BuiltinExchangeType.DIRECT);

        //绑定交换机
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName,ROUTE_EXCHANGE,INFO);
        channel.queueBind(queueName,ROUTE_EXCHANGE,ERROR);
        channel.queueBind(queueName,ROUTE_EXCHANGE,WARNING);


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
