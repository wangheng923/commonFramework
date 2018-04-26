package com.hyc.commandservice.lock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import redis.clients.jedis.JedisCommands;

import java.util.UUID;

/**
 * @Description:
 * @Author: wangheng2
 * @Date: 2018年04月25日
 * @Modified By:
 */
public class RedisDistributeLock implements DistributeLock {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private long expireTime;

    private int retryTime;
    @Autowired
    private RedisTemplate redisTemplate;

    public long getExpireTime() {
        return expireTime;
    }

    public void setExpireTime(long expireTime) {
        this.expireTime = expireTime;
    }

    public int getRetryTime() {
        return retryTime;
    }

    public void setRetryTime(int retryTime) {
        this.retryTime = retryTime;
    }

    @Override
    public boolean tryLock(String key) {
        return tryLock(key, expireTime);
    }

    @Override
    public boolean tryLock(String key, long time) {
        return tryLock(key, time, retryTime);
    }

    @Override
    public boolean tryLock(String key, long time, int retry) {
        boolean result = waitForLock(key, time);
        if (result) {
            return result;
        }
        for (int i = 0; i < retry; i++) {
            logger.debug("retry lock key:{} start,count:{}", key, i + 1);
            result = waitForLock(key, time);
            if (result) {
                logger.debug("retry lock key:{} success,count:{}", key, i + 1);
                return result;
            }
            logger.debug("retry lock key:{} end,count:{}", key, i + 1);
        }
        return false;
    }

    @Override
    public long releaseLock(String key) {
        try {
            Long result = (Long) redisTemplate.execute(new RedisCallback() {
                @Override
                public Long doInRedis(RedisConnection redisConnection) throws DataAccessException {
                    JedisCommands connection = (JedisCommands) redisConnection.getNativeConnection();
                    return connection.del(key);
                }
            });
            return result.longValue();
        } catch (Exception e) {
            return -1;
        }
    }

    private boolean waitForLock(String key, long time) {
        try {
            boolean setResult;
            long end = System.currentTimeMillis() + time;
            do {
                setResult = setKey(key, time);
            } while (!setResult && System.currentTimeMillis() < end);
            if (!setResult) {
                logger.debug("key:{}, waitForLock in {} milliseconds timeout", key, time);
            }
            return setResult;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean setKey(String key, long time) {
        String result = (String) redisTemplate.execute(new RedisCallback() {
            @Override
            public String doInRedis(RedisConnection redisConnection) throws DataAccessException {
                JedisCommands connection = (JedisCommands) redisConnection.getNativeConnection();
                return connection.set(key, UUID.randomUUID().toString(), "NX", "PX", time);
            }
        });
        return StringUtils.isNotBlank(result);
    }

}
