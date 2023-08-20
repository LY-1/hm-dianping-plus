package com.hmdp.service.mq;

import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIDWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class MQVoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIDWorker redisIDWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill_blockingQueue.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    @Autowired
    private TaskProvider taskProvider;

//    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

//    @PostConstruct
//    private void init() {
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandle());
//    }


    // 从阻塞队列中获取消息
//    private BlockingQueue<VoucherOrder> orderTasks= new ArrayBlockingQueue(1024 * 1024);
//    private class VoucherOrderHandle implements Runnable {
//
//        @Override
//        public void run() {
//            while (true) {
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();
//                    // 创建订单
//                    handleVoucherOrder(voucherOrder);
//                } catch (Exception e) {
//                    log.error("处理订单异常", e);
//                }
//            }
//        }
//    }

    @RabbitListener(queues = MQConstants.DIRECT_QUEUE_NAME)
    public void taskConsumer(String task) {
        VoucherOrder voucherOrder = JSON.parseObject(task, VoucherOrder.class);
        log.info("处理订单: {}", voucherOrder.toString());
        // test republish
//        System.out.println(1 / 0);
        try {
            // 创建订单
            handleVoucherOrder(voucherOrder);
        } catch (Exception e) {
            log.error("处理订单异常", e);
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 获取锁
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock("lcok:order" + userId);

        if (!lock.tryLock()) {
            log.error("获取锁失败");
            return;
        }
        try {
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    IVoucherOrderService proxy;

    // 阻塞队列
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 执行Lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT, Collections.emptyList(), voucherId.toString(), userId.toString());
        // 判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "请勿重复下单");
        }
        // 将优惠券ID、用户ID和订单ID存入阻塞队列

        // 保存到阻塞队列
        // 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 订单id
        long orderID = redisIDWorker.nextId("order");
        voucherOrder.setId(orderID);
        // 用户id
        voucherOrder.setUserId(userId);
        // 代金券id
        voucherOrder.setVoucherId(voucherId);


        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 添加到阻塞队列
//        orderTasks.add(voucherOrder);
        taskProvider.pusblish(voucherOrder);
        // 返回
        return Result.ok(orderID);
    }


    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            // 已经存在
            log.error("用户已经购买过优惠券");
            return;
        }
        // 扣减库存
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0)
                .update();
        if (!success) {
            log.error("优惠券库存不足");
            return;
        }
        // 保存
        save(voucherOrder);
    }
}
