package com.dzh.springrabbitmq.utils;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class RabbitMQ {

    public static Connection getConnection() {
        ConnectionFactory factory = new ConnectionFactory();
        //指定MQ服务端的地址
        factory.setHost("192.168.52.100");
        factory.setPort(5672);
        //用户名
        factory.setUsername("test");
        //密码
        factory.setPassword("test");

        factory.setVirtualHost("/test");
        Connection connection = null;
        try {
            connection = factory.newConnection();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
