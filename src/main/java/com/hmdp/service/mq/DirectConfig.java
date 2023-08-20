package com.hmdp.service.mq;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.retry.RepublishMessageRecoverer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DirectConfig {
    /**
     * state exchange
     * @return
     */
    @Bean
    public DirectExchange directExchange() {
        return new DirectExchange(MQConstants.DIRECT_EXCHANGE_NAME, true, false);
    }

    /**
     * state queue, durable default
     * @return
     */
    @Bean
    public Queue directQueue() {
        return new Queue(MQConstants.DIRECT_QUEUE_NAME);
    }

    /**
     * state binding relation
     * @param directQueue
     * @param directExchange
     * @return
     */
    @Bean
    public Binding bindingQueue(Queue directQueue, DirectExchange directExchange) {
        return BindingBuilder.bind(directQueue).to(directExchange).with(MQConstants.DIRECT_ROUTINGKEY);
    }

}
