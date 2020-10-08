package com.pure.redis.basic;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class BaseClass {
    public static Jedis getJedis(JedisPool jedisPool) {
        return jedisPool.getResource();
    }
    public static void closeJedis(Jedis jedis){
        if (jedis != null){
            jedis.close();
        }
    }
    public static void closePool(JedisPool pool){
        if (pool != null){
            pool.close();
        }
    }
}
