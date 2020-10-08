package com.pure.redis.util;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtis {
    private  static JedisPool jedisPool = null;
    public static JedisPool getJedisPool(){
        //使用默认配置8,8
        jedisPool = new JedisPool(new JedisPoolConfig(), "192.168.1.4");
        return jedisPool;
    }

}
