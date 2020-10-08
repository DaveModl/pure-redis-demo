package com.pure.redis.easycase.vote;

import com.pure.redis.basic.BaseClass;
import com.pure.redis.util.JedisUtis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ZParams;

import java.util.*;

/** 需求：文章排行榜,文章根据评分和发布时间排序 一个用户只能投票一次 key:{hash},key:{zset},文章投票key{set} */
public class ArticleVote extends BaseClass {
  /** 一周的有效期 */
  private static final int ONE_WEEK_SECONDS = 7 * 86400;
  /** 每一票的分数 */
  private static final double VOTE_SCORE = 432;
  /** 每页文章数 */
  private static final int ARTICLE_PRE_PAGE = 25;

  /**
   * 文章投票 反对票和票切换
   *
   * @param jedis
   * @param userId
   * @param articleId
   */
  public static void voteArticle(Jedis jedis, String userId, String articleId, Boolean isVoted) {
    long limit = System.currentTimeMillis() / 1000 - ONE_WEEK_SECONDS;
    if (jedis.zscore("postTime:", "article:" + articleId) < limit) {
      return;
    }
    // 新增投票
    if (isVoted) {
      if (jedis.sismember("devoted:" + articleId, userId)) {
        jedis.smove("devoted:" + articleId, "voted:" + articleId, userId);
      } else {
        jedis.sadd("voted:" + articleId, userId);
      }
      // 加分
      jedis.zincrby("articleScore:", VOTE_SCORE, "article:" + articleId);
      // 新增一票
      jedis.hincrBy("article:" + articleId, "votes", 1);
    } else {
      if (jedis.sismember("voted:" + articleId, userId)) {
        jedis.smove("voted:" + articleId, "devoted:" + articleId, userId);
      } else {
        jedis.sadd("devoted:" + articleId, userId);
      }
      jedis.zincrby("articleScore:", -VOTE_SCORE, "article:" + articleId);
      jedis.hincrBy("article:" + articleId, "devotes", 1);
    }
  }

  /**
   * 发布文章
   *
   * @param jedis
   * @param user
   * @param title
   * @param link
   * @return
   */
  public static String postArticle(Jedis jedis, String user, String title, String link) {
    // ArticleKey
    String articleId = String.valueOf(jedis.incr("article:"));
    String voted = "voted:" + articleId;
    // 自己也算一票
    jedis.sadd(voted, user);
    jedis.expire(voted, ONE_WEEK_SECONDS);
    long now = System.currentTimeMillis() / 1000;
    Map<String, String> articleMap = new HashMap<>();
    articleMap.put("title", title);
    articleMap.put("link", link);
    articleMap.put("author", user);
    articleMap.put("postTime", String.valueOf(now));
    articleMap.put("votes", "1");
    articleMap.put("devotes", "0");
    jedis.hset("article:" + articleId, articleMap);
    // 初始化分数
    jedis.zadd("articleScore:", (now + VOTE_SCORE), "article:" + articleId);
    // 文章排序
    jedis.zincrby("postTime:", now, "article:" + articleId);
    return articleId;
  }

  /**
   * 根据分值获取文章
   *
   * @param jedis
   * @param page
   * @param order
   * @return
   */
  public static List<Map<String, String>> getArticles(Jedis jedis, int page, String order) {
    int start = (page - 1) * ARTICLE_PRE_PAGE;
    int end = start + ARTICLE_PRE_PAGE - 1;
    // 降序获取,article:articleId
    Set<String> keys = jedis.zrevrange(order, start, end);
    List<Map<String, String>> res = new ArrayList<>();
    keys.forEach(
        key -> {
          Map<String, String> article = jedis.hgetAll(key);
          article.put("articleId", key);
          res.add(article);
        });
    return res;
  }

  /**
   * 文章添加分组
   *
   * @param jedis
   * @param articleId
   * @param groups
   */
  public static void addGroup(Jedis jedis, String articleId, String[] groups) {
    String article = "article:" + articleId;
    for (String group : groups) {
      jedis.sadd("articleGroup:" + group, article);
    }
  }

  /**
   * 获取分组文章 聚合zinterstore依据分值取交集
   *
   * @param jedis
   * @param group
   * @param page
   * @param order
   * @return
   */
  public static List<Map<String, String>> getGroupArticles(
      Jedis jedis, String group, int page, String order) {
    // 最大值取交集
    String key = order + group;
    if (!jedis.exists(key)) {
      // 聚合最大值
      ZParams zParams = new ZParams().aggregate(ZParams.Aggregate.MAX);
      jedis.zinterstore(key, zParams, "group:" + group, order);
      jedis.expire(key, 60);
    }
    return getArticles(jedis, page, order);
  }

  private static void showArticles(List<Map<String, String>> articles) {
    for (Map<String, String> article : articles) {
      article.forEach((k, v) -> System.out.println("key:" + k + "," + "value:" + v));
    }
  }

  public static void run() {
    JedisPool jedisPool = null;
    Jedis jedis = null;
    try {
      jedisPool = JedisUtis.getJedisPool();
      jedis = getJedis(jedisPool);
      jedis.select(15);
      String articleId = postArticle(jedis, "username", "A title", "http://www.google.com");
      System.out.println("We posted a new article with id: " + articleId);
      System.out.println("Its HASH looks like:");
      Map<String, String> articleData = jedis.hgetAll("article:" + articleId);
      articleData.forEach((k, v) -> System.out.println("key:" + k + "," + "value:" + v));
      System.out.println();
      voteArticle(jedis, "other_user", articleId, true);
      String votes = jedis.hget("article" + articleId, "voted");
      System.out.println("We voted for the article, it now has votes: " + votes);

      voteArticle(jedis, "other_user_devoted", articleId, false);
      String devotes = jedis.hget("article:" + articleId, "devoted");
      System.out.println("We devoted for the article, it now has devotes: " + devotes);

      System.out.println();

      System.out.println("The currently highest-scoring articles are:");
      List<Map<String, String>> articles = getArticles(jedis, 1, "articleScore:");
      showArticles(articles);

      addGroup(jedis, articleId, new String[] {"new-group"});
      System.out.println("We added the article to a new group, other articles include:");
      articles = getGroupArticles(jedis, "new-group", 1, "articleScore:");
      showArticles(articles);
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      jedis.close();
    jedisPool.close();
    }
  }

  public static void main(String[] args) {
    run();
  }
}
