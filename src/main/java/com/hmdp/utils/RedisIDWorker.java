package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIDWorker {
    private StringRedisTemplate stringRedisTemplate;

    // 2022.1.1 00:00:00开始
    private static final long start = 1640995200L;
    // 序列号位数
    private static final long COUNT_BITS = 32;

    public RedisIDWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId (String keyPrefix) {
        // 生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long end = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = end - start;

        // 生成序列号
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        // 拼接并返回
        return timeStamp << COUNT_BITS | count;
    }

}
