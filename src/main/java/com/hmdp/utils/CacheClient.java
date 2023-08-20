package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.sql.Time;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private static final ExecutorService CACHE_REBUUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    // 将java对象序列化为json并存储在string类型的key中，可设置TTL过期时间
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    // 将java对象序列化为json并存储在string类型的key中，可设置逻辑过期时间
    public void setWithLogicalExpire (String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期时间，将逻辑过期时间和对象绑定到新的对象中
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    // 缓存击穿
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type,
                                          Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        // 从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(keyPrefix + id);

        // 如果redis中有缓存，即命中，则返回
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, type);
        }
        // 判断是否命中空值，null和""是不一样的，如果非null且isNotBlank的话，则为""
        if (shopJson != null) {
            return null;
        }

        // redis中没有缓存，则查数据库
        R shop = dbFallback.apply(id);
        if (shop == null) {
            // 缓存空对象
            stringRedisTemplate.opsForValue().set(keyPrefix + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);

            // 数据库中不存在，返回错误
            return null;
        }
        // 数据库中存在，则写入缓存，并返回
        this.set(keyPrefix + id, shop, time, unit);
        return shop;
    }

    // 逻辑过期解决缓存击穿问题
    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type,
                                            Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        // 从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(keyPrefix + id);

        // 如果redis中没有缓存，则返回
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }

        // 存在，反序列化
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        R shop = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 判断缓存是否过期
        if (LocalDateTime.now().isBefore(expireTime)) {
            // 没过期则返回商铺信息
            return shop;
        }
        // 过期了，去获取互斥锁
        // 没有获取到，返回商铺信息
        if (!tryLock(LOCK_SHOP_KEY + id)) {
            return shop;
        }
        // 获取到了，先做Double Check，看是否有线程已经做了缓存重建了
        // 判断缓存是否过期
        if (LocalDateTime.now().isBefore(expireTime)) {
            // 没过期则返回商铺信息
            return shop;
        }
        // 还是过期，说明还没有线程缓存重建，开启线程，执行查询，进行缓存重建
        CACHE_REBUUILD_EXECUTOR.submit(()->{
            // 将商铺数据写入Redis，并重置过期时间
            try {
                R newR = dbFallback.apply(id);
                this.setWithLogicalExpire(keyPrefix + id, newR, time, unit);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                // 释放锁
                unlock(LOCK_SHOP_KEY + id);
            }

        });
        return shop;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

}
