package com.dzh.springrabbitmq;

import com.dzh.springrabbitmq.utils.RabbitMQ;
import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

@SpringBootTest
class SpringRabbitmqApplicationTests {

    public static final String HELLO = "hello";

    @Test
    void mqConnectionTest() {
        System.out.println(RabbitMQ.getConnection());
    }

    /**
     * 生产者发布消息 批量confirm
     */
    @Test
    void publish() throws IOException, TimeoutException, InterruptedException {
        //1.获取连接
        Connection connection = RabbitMQ.getConnection();
        //2.创建连接管道
        Channel channel = connection.createChannel();

        channel.confirmSelect();

        //3.发布消息
        String msg = "干嘛呢";


//        channel.basicPublish("", HELLO, null, msg.getBytes());
//
//        //消息发布完成后，确认消息是否投递到交换机
//        if (channel.waitForConfirms()) {
//            System.out.println("消息投递到交换机啦 OK");
//        } else {
//            System.out.println("消息投递失败了 FAIL");
//            channel.basicPublish("", HELLO, null, msg.getBytes());
//
//        }

        for (int i = 0; i < 500; i++) {
            channel.basicPublish("", HELLO, null, msg.getBytes());
        }


        //批量消息发布成功后，统一确认消息发布状态   批量的confirm机制
        channel.waitForConfirmsOrDie();
        System.out.println("消息确认全部都投递到交换机了！");

        System.out.println("生产者发布消息成功");

        //4.关闭资源
        channel.close();
        connection.close();
    }

    /**
     * 异步
     */

    @Test
    void publish02() throws IOException, TimeoutException, InterruptedException {
        //1.获取连接
        Connection connection = RabbitMQ.getConnection();
        //2.创建连接管道
        Channel channel = connection.createChannel();

        channel.confirmSelect();

        //3.发布消息
        String msg = "干嘛呢";

        // 添加confirm监听
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long l, boolean b) throws IOException {
                // 消息成功投递到交换机
                System.out.println("消息投递成功");
                System.out.println("消息的标识:" + l + ",消失是否是批量确认:" + b);
            }

            @Override
            public void handleNack(long l, boolean b) throws IOException {
                // 消息投递到交换机失败
                System.out.println("消息投递失败,可以进行重试..");
                System.out.println("消息的标识:" + l + ",消失是否是批量确认:" + b);
                // 重试消息
                sendMsg(channel);
            }
        });

        //  3、发布消息
        sendMsg(channel);


        System.out.println("生产者发布消息成功");

        System.in.read();
        //4.关闭资源
        channel.close();
        connection.close();
    }

    // 发送消息的方法
    private void sendMsg(Channel channel) throws IOException {
        String msg = "Hello,World222!";
        for (int i = 0; i < 500; i++) {
            channel.basicPublish("", HELLO, null, msg.getBytes());
        }
    }


    /**
     * 消费者
     */
    @Test
    void consumer() throws IOException, TimeoutException {
        //1.创建连接
        Connection connection = RabbitMQ.getConnection();

        //创建管道
        Channel channel = connection.createChannel();

        //3.管道绑定
        channel.queueDeclare(HELLO, true, false, true, null);

        //4.消费
        channel.basicConsume(HELLO, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                //body 就是消费者消费的消费对象
                String msg = new String(body, StandardCharsets.UTF_8);
                System.out.println("消费者接收到的消息是：" + msg);
            }

        });

        System.out.println("消费者开始监听队列！");
        System.in.read();
        channel.close();

        connection.close();

    }

}
