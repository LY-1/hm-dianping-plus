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
public class ErrorMessageConfig {
    /**
     * state exchange
     * @return
     */
    @Bean
    public DirectExchange errorExchange() {
        return new DirectExchange(MQConstants.ERROR_EXCHANGE_NAME, true, false);
    }

    /**
     * state queue, durable default
     * @return
     */
    @Bean
    public Queue errorQueue() {
        return new Queue(MQConstants.ERROR_QUEUE_NAME);
    }

    /**
     * state binding relation
     * @param errorQueue
     * @param errorExchange
     * @return
     */
    @Bean
    public Binding bindingErrorQueue(Queue errorQueue, DirectExchange errorExchange) {
        return BindingBuilder.bind(errorQueue).to(errorExchange).with(MQConstants.ERROR_ROUTINGKEY);
    }


    @Bean
    public MessageRecoverer republishMessageRecoverer(RabbitTemplate rabbitTemplate) {
        return new RepublishMessageRecoverer(rabbitTemplate, MQConstants.ERROR_EXCHANGE_NAME, MQConstants.ERROR_ROUTINGKEY);
    }

}
