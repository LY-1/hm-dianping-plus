package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{
    private StringRedisTemplate stringRedisTemplate;
    private String name;

    public SimpleRedisLock(StringRedisTemplate stringRedisTemplate, String name) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.name = name;
    }

    private static final String KEY_PREFIX = "lock:";
    private static final String ID_PRIFIX = UUID.randomUUID().toString(true) + "-";


    @Override
    public boolean tryLock(long time) {
        String threadId = ID_PRIFIX + Thread.currentThread().getId();
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadId, time, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(success);
    }

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }
    @Override
    public void unlock() {
        stringRedisTemplate.execute(UNLOCK_SCRIPT,
                Collections.singletonList(KEY_PREFIX + name), ID_PRIFIX + Thread.currentThread().getId());
    }


    /* @Override
    public void unlock() {
        String threadId = ID_PRIFIX + Thread.currentThread().getId();
        if (threadId.equals(stringRedisTemplate.opsForValue().get(KEY_PREFIX + name))) {
            stringRedisTemplate.delete(KEY_PREFIX + name);
        }
    }*/
}
