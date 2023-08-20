package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.sql.Time;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryByID(Long id) {
        /*
        // 缓存击穿
        // Shop shop = queryWithPassThrough(id);

        // 互斥锁解决缓存击穿
        // Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存击穿
        Shop shop = queryWithLogicalExpire(id);
        if (shop == null) return Result.fail("店铺不存在！");
        return Result.ok(shop);*/

        // 工具类测试
        CacheClient cacheClient = new CacheClient(stringRedisTemplate);
//        Shop shop = cacheClient.queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        Shop shop = cacheClient.queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);
        if (shop == null) return Result.fail("店铺不存在！");
        return Result.ok(shop);
    }

    /*private static final ExecutorService CACHE_REBUUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public Shop queryWithLogicalExpire(Long id) {
        // 从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // 如果shopJson为null或者""，则isBlank返回true
        if (StrUtil.isBlank(shopJson)) {
            return null;
        }

        // 反序列化
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
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
                this.saveShop2Redis(id, 20L);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                // 释放锁
                unlock(LOCK_SHOP_KEY + id);
            }

        });
        return shop;
    }


    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        // 根据id查询数据库
        Shop shop = getById(id);
        Thread.sleep(200);
        // 封装过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        // 保存到Redis

        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    public Shop queryWithMutex(Long id) {
        // 从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // 如果redis中有缓存，即命中，则返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 判断是否命中空值，null和""是不一样的，如果非null且非isNotBlank的话，则为""
        if (shopJson != null) {
            return null;
        }

        Shop shop = null;
        // redis中没有缓存，则查数据库，缓存重建
        // 尝试获取互斥锁
        String lock = LOCK_SHOP_KEY + id;
        try {
            // 判断是否获取到锁
            if (!tryLock(lock)) {
                // 没有获取到，休眠一段时间，重新查询
                Thread.sleep(50);
                queryWithMutex(id);
            }
            // 获取到了，先做Double check
            shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

            // 如果redis中有缓存，即命中，则返回
            if (StrUtil.isNotBlank(shopJson)) {
                shop = JSONUtil.toBean(shopJson, Shop.class);
                return shop;
            }
            // 判断是否命中空值，null和""是不一样的，如果非null且非isNotBlank的话，则为""
            if (shopJson != null) {
                return null;
            }
            // 如果Double Check还是没有，再根据id查询数据库
            shop = getById(id);
            Thread.sleep(200);
            if (shop == null) {
                // 缓存空对象
                stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 数据库中不存在，返回错误
                return null;
            }
            // 数据库中存在，则写入缓存，并返回
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放互斥锁，并返回
            unlock(lock);
        }
        return shop;
    }

    public Shop queryWithPassThrough(Long id) {
        // 从redis中查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // 如果redis中有缓存，即命中，则返回
        if (StrUtil.isNotBlank(shopJson)) {
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // 判断是否命中空值，null和""是不一样的，如果非null且非isNotBlank的话，则为""
        if (shopJson != null) {
            return null;
        }

        // redis中没有缓存，则查数据库
        Shop shop = getById(id);
        if (shop == null) {
            // 缓存空对象
            stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);

            // 数据库中不存在，返回错误
            return null;
        }
        // 数据库中存在，则写入缓存，并返回
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }*/

    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }

        // 更新数据库
        updateById(shop);

        // 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);

        return null;
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 判断x和y是否非空
        if (x == null || y == null) {
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        // 计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 根据经纬度查询店铺
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 结果非空判断
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        // 没有下一页了，结束
        if (from >= list.size()) {
            return Result.ok(Collections.emptyList());
        }
        List<String> ids = new ArrayList<>(list.size());
        Map<String, Distance> map = new HashMap<>(list.size());
        list.stream().skip(from).forEach(
                result->{
                    // 存放店铺id
                    String shopId = result.getContent().getName();
                    ids.add(shopId);

                    // 存放店铺即对应的距离
                    Distance distance = result.getDistance();
                    map.put(shopId, distance);
                }
        );
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("order by field(id, " + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(map.get(shop.getId().toString()).getValue());
        }

        return Result.ok(shops);
    }
}
