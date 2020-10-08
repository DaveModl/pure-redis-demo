package com.pure.redis.easycase.web;

import lombok.SneakyThrows;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ZParams;

public class DecrView extends Thread {
  private Jedis jedis;
  private boolean quit;

  public DecrView(Jedis jedis) {
      this.jedis = jedis;
      this.jedis.select(14);
  }

  public void quit() {
    this.quit = true;
  }

  @SneakyThrows
  @Override
  public void run() {
    while (!quit) {
        jedis.zremrangeByRank("viewed:",0,-20001);
        ZParams zParams = new ZParams().weights(0.5).aggregate(ZParams.Aggregate.MAX);
        jedis.zinterstore("viewed:",zParams,"viewed:");
        Thread.sleep(300L);
    }
  }
}
