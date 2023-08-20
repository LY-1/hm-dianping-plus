package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIDWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

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



    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandle());
    }

/*
    // 获取消息队列中的消息
    private class VoucherOrderHandle implements Runnable {
        String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    // 获取消息队列中的消息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    // 判断是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 获取失败继续下一次循环
                        continue;
                    }

                    // 获取成功，处理消息，解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 创建订单
                    handleVoucherOrder(voucherOrder);

                    //ACK确认，SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    // 获取PendingList中的消息 XREADGROUP GROUP g1 c1 COUNT 1 stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    // 判断是否获取成功
                    if (list == null || list.isEmpty()) {
                        // 获取失败，说明pendingList中没有消息，退出循环
                        break;
                    }

                    // 获取成功，处理消息，解析消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 创建订单
                    handleVoucherOrder(voucherOrder);

                    //ACK确认，SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("处理pending-list异常", e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
            }
        }
    }*/

    // 从阻塞队列中获取消息
    private BlockingQueue<VoucherOrder> orderTasks= new ArrayBlockingQueue(1024 * 1024);
    private class VoucherOrderHandle implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
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

    // 配合消息队列的
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户ID
        Long userId = UserHolder.getUser().getId();
        // 获取订单ID
        long orderID = redisIDWorker.nextId("order");
        // 执行Lua脚本
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderID));
        // 判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(r == 1 ? "库存不足" : "请勿重复下单");
        }
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 返回
        return Result.ok(orderID);
    }*/

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
        orderTasks.add(voucherOrder);
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

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 查询代金券信息
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀尚未开始");
        }

        // 判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束");
        }
        // 判断优惠券库存是否充足
        if (voucher.getStock() < 1) {
            return Result.fail("优惠券库存不足");
        }
        // 判断订单是否已经存在
        Long userId = UserHolder.getUser().getId();

        // 获取锁
//        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order" + userId);
        RLock lock = redissonClient.getLock("lcok:order" + userId);

        if (!lock.tryLock()) {
            return Result.fail("请勿重复下单");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService)AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } catch (IllegalStateException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

    }*/

    /*@Transactional
    public Result createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = UserHolder.getUser().getId();
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherOrder).count();
        if (count > 0) {
            // 已经存在
            return Result.fail("用户已经购买过优惠券");
        }
        // 扣减库存
        boolean success = seckillVoucherService.update().setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder).gt("stock", 0)
                .update();
        if (!success) {
            return Result.fail("优惠券库存不足");
        }
        // 创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 订单id
        long orderID = redisIDWorker.nextId("order");
        voucherOrder.setId(orderID);
        // 用户id
        voucherOrder.setUserId(UserHolder.getUser().getId());
        // 代金券id
        voucherOrder.setVoucherId(voucherOrder);
        // 保存
        save(voucherOrder);
        // 返回订单ID
        return Result.ok(orderID);
    }*/
}
