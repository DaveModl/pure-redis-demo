package com.pure.redis.easycase.web;

import com.pure.redis.basic.BaseClass;
import com.pure.redis.util.JedisUtis;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** 令牌cookie,信息存在redis */
public class WebCache extends BaseClass {
  /**
   * 尝试获取token
   *
   * @param jedis
   * @param token
   * @return
   */
  public String checkToken(Jedis jedis, String token) {
    return jedis.hget("login:", token);
  }

  /**
   * 更新cookie
   *
   * @param jedis
   * @param token
   * @param user
   * @param product
   */
  public void updateToken(Jedis jedis, String token, String user, String product) {
    Long timestamp = System.currentTimeMillis() / 1000;
    // 用户令牌
    jedis.hset("login:", token, user);
    // 保存最近一次的token
    jedis.zadd("recent:", timestamp, token);
    if (!StringUtils.isBlank(product)) {
      // 访问过的商品
      jedis.zadd("viewed:" + token, timestamp, product);
      // 最近浏览的25个商品
      jedis.zremrangeByRank("viewed:" + token, 0, -26);
      jedis.zincrby("viewed:", -1, product);
    }
  }

  /**
   * 购物车商品hash
   *
   * @param jedis
   * @param session
   * @param product
   * @param count
   */
  public void addCart(Jedis jedis, String session, String product, int count) {
    if (count <= 0) {
      jedis.hdel("cart:" + session, product);
    } else {
      jedis.hset("cart:" + session, product, String.valueOf(count));
    }
  }

  /**
   * 缓存页面请求
   *
   * @param jedis
   * @param callback
   * @param request
   * @return
   */
  public String cacheRequest(Jedis jedis, Callback callback, String request) {
    if (!canCache(jedis, request)) {
      return callback != null ? callback.call(request) : null;
    }
    String pageKey = "cache:" + hashRequest(request);
    String content = jedis.get(pageKey);
    if (StringUtils.isBlank(content) && callback != null) {
      content = callback.call(request);
      jedis.setex(pageKey, 300, content);
    }
    return content;
  }

  /**
   * JSON
   *
   * @param conn
   * @param rowId
   * @param delay
   */
  public void scheduleRowCache(Jedis conn, String rowId, int delay) {
    conn.zadd("delay:", delay, rowId);
    conn.zadd("schedule:", System.currentTimeMillis() / 1000, rowId);
  }

  private String hashRequest(String request) {
    return String.valueOf(request.hashCode());
  }

  private Boolean canCache(Jedis jedis, String request) {
    try {
      URL url = new URL(request);
      Map<String, String> params = new HashMap<>();
      if (!StringUtils.isBlank(url.getQuery())) {
        // 取参数
        for (String param : url.getQuery().split("&")) {
          String[] pair = param.split("=", 2);
          params.put(pair[0], pair.length == 2 ? pair[1] : null);
        }
      }
      String prodId = extractProdId(params);
      if (prodId == null || isDynamic(params)) {
        return false;
      }
      Long rank = jedis.zrank("viewed:", prodId);
      return rank != null && rank < 10000;
    } catch (MalformedURLException e) {
      return false;
    }
  }

  private boolean isDynamic(Map<String, String> params) {
    return params.containsKey("_");
  }

  private String extractProdId(Map<String, String> params) {
    return params.get("prod");
  }

  public void testLoginCookies(Jedis jedis) throws InterruptedException {
    System.out.println("\n----- testLoginCookies -----");
    String token = UUID.randomUUID().toString();

    updateToken(jedis, token, "username", "prod");
    System.out.println("We just logged-in/updated token: " + token);
    System.out.println("For user: 'username'");
    System.out.println();

    System.out.println("What username do we get when we look-up that token?");
    String r = checkToken(jedis, token);
    System.out.println(r);
    System.out.println();
    assert r != null;

    System.out.println("Let's drop the maximum number of cookies to 0 to clean them out");
    System.out.println("We will start a thread to do the cleaning, while we stop it later");

    CleanSession thread = new CleanSession(0, jedis);
    thread.start();
    Thread.sleep(1000);
    thread.quit();
    Thread.sleep(2000);
    if (thread.isAlive()) {
      throw new RuntimeException("The clean sessions thread is still alive?!?");
    }

    long s = jedis.hlen("login:");
    System.out.println("The current number of sessions still available is: " + s);
    assert s == 0;
  }

  public void testShopppingCartCookies(Jedis jedis) throws InterruptedException {
    System.out.println("\n----- testShopppingCartCookies -----");
    String token = UUID.randomUUID().toString();

    System.out.println("We'll refresh our session...");
    updateToken(jedis, token, "username", "itemX");
    System.out.println("And add an item to the shopping cart");
    addCart(jedis, token, "itemY", 3);
    Map<String, String> r = jedis.hgetAll("cart:" + token);
    System.out.println("Our shopping cart currently has:");
    for (Map.Entry<String, String> entry : r.entrySet()) {
      System.out.println("  " + entry.getKey() + ": " + entry.getValue());
    }
    System.out.println();

    assert r.size() >= 1;

    System.out.println("Let's clean out our sessions and carts");
    CleanSessionCart thread = new CleanSessionCart(0, jedis);
    thread.start();
    Thread.sleep(1000);
    thread.quit();
    Thread.sleep(2000);
    if (thread.isAlive()) {
      throw new RuntimeException("The clean sessions thread is still alive?!?");
    }

    r = jedis.hgetAll("cart:" + token);
    System.out.println("Our shopping cart now contains:");
    for (Map.Entry<String, String> entry : r.entrySet()) {
      System.out.println("  " + entry.getKey() + ": " + entry.getValue());
    }
    assert r.size() == 0;
  }

  public void testCacheRows(Jedis jedis) throws InterruptedException {
    System.out.println("\n----- testCacheRows -----");
    System.out.println("First, let's schedule caching of itemX every 5 seconds");
    scheduleRowCache(jedis, "itemX", 5);
    System.out.println("Our schedule looks like:");
    Set<Tuple> s = jedis.zrangeWithScores("schedule:", 0, -1);
    for (Tuple tuple : s) {
      System.out.println("  " + tuple.getElement() + ", " + tuple.getScore());
    }
    assert s.size() != 0;

    System.out.println("We'll start a caching thread that will cache the data...");

    CacheRows thread = new CacheRows(jedis);
    thread.start();

    Thread.sleep(1000);
    System.out.println("Our cached data looks like:");
    String r = jedis.get("inv:itemX");
    System.out.println(r);
    assert r != null;
    System.out.println();

    System.out.println("We'll check again in 5 seconds...");
    Thread.sleep(5000);
    System.out.println("Notice that the data has changed...");
    String r2 = jedis.get("inv:itemX");
    System.out.println(r2);
    System.out.println();
    assert r2 != null;
    assert !r.equals(r2);

    System.out.println("Let's force un-caching");
    scheduleRowCache(jedis, "prod", -1);
    Thread.sleep(1000);
    r = jedis.get("inv:itemX");
    System.out.println("The cache was cleared? " + (r == null));
    assert r == null;

    thread.quit();
    Thread.sleep(2000);
    if (thread.isAlive()) {
      throw new RuntimeException("The database caching thread is still alive?!?");
    }
  }

  public void testCacheRequest(Jedis conn) {
    System.out.println("\n----- testCacheRequest -----");
    String token = UUID.randomUUID().toString();

    Callback callback = request -> "content for " + request;

    updateToken(conn, token, "username", "itemX");
    String url = "http://test.com/?item=itemX";
    System.out.println("We are going to cache a simple request against " + url);
    String result = cacheRequest(conn, callback, url);
    System.out.println("We got initial content:\n" + result);
    System.out.println();

    assert result != null;

    System.out.println("To test that we've cached the request, we'll pass a bad callback");
    String result2 = cacheRequest(conn, null, url);
    System.out.println("We ended up getting the same response!\n" + result2);

    assert result.equals(result2);

    assert !canCache(conn, "http://test.com/");
    assert !canCache(conn, "http://test.com/?item=itemX&_=1234536");
  }

  public void run() {
    Jedis jedis = getJedis(JedisUtis.getJedisPool());
    jedis.select(14);
    try {
      testLoginCookies(jedis);
      testShopppingCartCookies(jedis);
      testCacheRows(jedis);
      testCacheRequest(jedis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    WebCache webCache = new WebCache();
    webCache.run();
  }
}
