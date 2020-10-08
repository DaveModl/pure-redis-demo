package com.pure.redis.basic;

import com.pure.redis.util.JedisUtis;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** 基本数据类型操作测试 */
@Slf4j
public class DataOptTest extends BaseClass {
  /** 基本字符串操作k-v */
  public static void stringOpt() {
    Jedis jedis = null;
    JedisPool jedisPool = null;
    try {
      jedisPool = JedisUtis.getJedisPool();
      jedis = getJedis(jedisPool);
      String status = jedis.set("hello", "world");
      log.info("status:{}", status);
      log.info(jedis.get("hello"));
      Long num = jedis.del("hello");
      log.info("dels num:{}", num);
    } catch (Exception e) {
      log.error("error", e);
    } finally {
      closeJedis(jedis);
    }
    closePool(jedisPool);
  }

  /** 基本列表操作k-{v1..vn}可重复.插入的顺序 */
  public static void listOpt() {
    try (JedisPool jedisPool = JedisUtis.getJedisPool();
        Jedis jedis = getJedis(jedisPool)) {
      // push
      jedis.rpush("list-key", "item1", "item2", "item3");
      // getAll
      List<String> list = jedis.lrange("list-key", 0, -1);
      list.forEach(log::info);
      // getOne
      String str = jedis.lindex("list-key", 2);
      log.info("index-3:{}", str);
      // pop
      String pop = jedis.lpop("list-key");
      log.info("pop elem:{}", pop);
      list = jedis.lrange("list-key", 0, -1);
      list.forEach(log::info);
    }
  }

  /** 基本集合操作k-{v1...vn}不重复,无序 */
  public static void setOpt() {
    Jedis jedis = null;
    JedisPool jedisPool = null;
    try {
      jedisPool = JedisUtis.getJedisPool();
      jedis = getJedis(jedisPool);
      jedis.sadd("set-key", "set-item1", "set-item2");
      Long addElems = jedis.sadd("set-key", "set-item1");
      log.info("add elem num:{}", addElems);
      // getAll
      Set<String> smembers = jedis.smembers("set-key");
      smembers.forEach(s -> log.info("member:{}", s));
      // exist
      Boolean item = jedis.sismember("set-key", "item");
      log.info("[item] exist?{}", item);
      // remove
      Long srem = jedis.srem("set-key", "set-item2");
      log.info("remove mem:{}", srem);
    } catch (Exception e) {
      log.error("error", e);
    } finally {
      closeJedis(jedis);
    }
    closePool(jedisPool);
  }

  /** 基本哈希操作key{sub-key val1,sub-key2,val2}... key不重复*/
  public static void hashOpt() {
    Jedis jedis = null;
    JedisPool jedisPool = null;
    try {
      jedisPool = JedisUtis.getJedisPool();
      jedis = getJedis(jedisPool);
      Map<String, String> map = new HashMap<>();
      map.put("key1", "val1");
      map.put("key2", "val2");
      jedis.hset("hash-key", map);
      jedis.hset("hash-key", "key", "val");
      Map<String, String> res = jedis.hgetAll("hash-key");
      res.forEach((k, v) -> log.info("key:{},value:{}", k, v));
      String val = jedis.hget("hash-key", "key");
      log.info("[key]:{}", val);
      Long hdel = jedis.hdel("hash-key", "key2");
      log.info("dels:{}", hdel);
      res = jedis.hgetAll("hash-key");
      res.forEach((k, v) -> log.info("key:{},value:{}", k, v));
    } catch (Exception e) {
      log.error("error", e);
    } finally {
      closeJedis(jedis);
    }
    closePool(jedisPool);
  }

  /** zset基本操作key:{member0：score0...} 不重复分值排序*/
  public static void sortedSetOpt() {
    Jedis jedis = null;
    JedisPool jedisPool = null;
    try {
      jedisPool = JedisUtis.getJedisPool();
      jedis = getJedis(jedisPool);
      jedis.zadd("zset-key", 567D, "mem1");
      jedis.zadd("zset-key", 676D, "mem2");
      jedis.zadd("zset-key", 454D, "mem3");
      Set<Tuple> tuples = jedis.zrangeWithScores("zset-key", 0L, -1L);
      tuples.forEach(t -> log.info("mem:{},score:{}", t.getElement(), t.getScore()));
      Set<Tuple> tuples2 = jedis.zrangeByScoreWithScores("zset-key", 300D, 600D);
      tuples2.forEach(t2 -> log.info("mem:{},score:{}", t2.getElement(), t2.getScore()));
      jedis.zrem("zset-key", "mem2");
      Set<String> set = jedis.zrange("zset-key", 0, -1);
      set.forEach(s -> log.info("zset:{}", set));
    } catch (Exception e) {
      log.error("error", e);
    } finally {
      closeJedis(jedis);
    }
    closePool(jedisPool);
  }

  public static void main(String[] args) {
    // stringOpt();
    // listOpt();
    // setOpt();
    // hashOpt();
    sortedSetOpt();
  }
}
