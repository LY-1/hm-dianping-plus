package com.hmdp.service.mq;

import com.alibaba.fastjson.JSON;
import com.hmdp.entity.VoucherOrder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TaskProvider {

    @Autowired
    private RabbitTemplate rabbitTemplate;

    public void pusblish(VoucherOrder voucherOrder) {
        // add ConfirmCallback, you can specify when sending a message
        // add global unique message ID
        CorrelationData correlationData = new CorrelationData(voucherOrder.getVoucherId().toString());
        correlationData.getFuture().addCallback(
                result -> {
                    // ack, send success
                    if (result.isAck()) {
                        log.info("order send successfully, ID: {}", correlationData.getId());
                    } else {
                        // nack, send fail
                        log.error("order send fail, ID: {}, reason: {}", correlationData.getId(), result.getReason());
                    }
                },
                ex -> log.error("order send fail, ID: {}, reason: {}", correlationData.getId(), ex.getMessage())
        );

        String jsonString = JSON.toJSONString(voucherOrder);

        // message durable default
        rabbitTemplate.convertAndSend(MQConstants.DIRECT_EXCHANGE_NAME, MQConstants.DIRECT_ROUTINGKEY, jsonString, correlationData);
    }
}
