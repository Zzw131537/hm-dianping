package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Author: Zhouzw
 * Date: 2025/3/2 17:30
 */
@Component
@Slf4j
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit timeUnit){

        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,timeUnit);
    }

    public void setWithLogicalExpire(String key,Object value,Long time,TimeUnit timeUnit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(timeUnit.toSeconds(time)));

        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));

    }

    // 缓存穿透
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback,Long time,TimeUnit timeUnit) {
        String key = keyPrefix + id;
        // 1.从redis 查询商品缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否存在
        if (StrUtil.isNotBlank(json)) {
            //3. 存在直接返回
           return JSONUtil.toBean(json,type);
        }
        // 判断命中是否是空值
        if(json != null){ // 相当于 == ""
            // 返回错误信息
            return null;
        }
        //4. 不存在，根据id查询数据库
        R r =dbFallback.apply(id);

        //5. 不存在,返回 错误
        if (r == null) {
            // 缓存穿透
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //6.存在写入redis
       this.set(key,r,time,timeUnit);

        //7.返回
        return r;

    }


    // 缓存线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    // 逻辑过期
    public <R,ID> R queryWithLogicalExpire(String kerPrefix ,ID  id,Class<R> type,Function<ID,R>dbFallback,Long time,TimeUnit timeUnit) {
         String key = kerPrefix+id;

        // 1.从redis 查询商品缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        //2. 判断是否存在
        if (StrUtil.isBlank(json)) {
            //3. 存在直接返回
            return null;
        }

        //命中，先反序列化
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);

        LocalDateTime expireTime = redisData.getExpireTime();

        // 判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 未过期 直接返回
            return r;
        }

        // 已过期缓存重建

        // 获取互斥锁
        String lockKey = "lock:shop"+id;
        boolean isLock = tryLock(lockKey);
        // 判断是否获取成功
        if(isLock) {
            // 成功,开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                // 重建缓存
                try {
                    // 写入数据库
                 R r1 = dbFallback.apply(id);
                 this.setWithLogicalExpire(key,r1,time,timeUnit);

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    // 释放锁
                    unLock(lockKey);
                }
            });
        }
        // 失败,返回旧数据
        return r;
    }
    // 设置锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", RedisConstants.LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    // 删除锁
    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

}
