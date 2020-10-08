package com.pure.redis.easycase.web;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Set;

public class CleanSession extends Thread {
    private Jedis jedis;
    private Integer limit;
    private boolean quit;

    public CleanSession(int limit,Jedis jedis){
        this.jedis = jedis;
        this.jedis.select(14);
        this.limit = limit;
    }

    public void quit(){
        this.quit =true;
    }

    /**
     * 用户如果正在访问网站可能出问题
     * 竞态条件
     */
    @Override
    public void run() {
        while (!quit){
            Long size = jedis.zcard("recent:");
            if (size <= limit){
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                continue;
            }

            long endIndex = Math.min(size - limit,100);
            //按时间排序的token
            Set<String> tokenSet = jedis.zrange("recent:", 0, endIndex - 1);
            String[] tokens = tokenSet.toArray(new String[tokenSet.size()]);

            ArrayList<String> sessionKeys = new ArrayList<>();
            for (String token : tokens) {
                sessionKeys.add("viewed:" + token);
            }

            jedis.del(sessionKeys.toArray(new String[sessionKeys.size()]));
            jedis.hdel("login:", tokens);
            jedis.zrem("recent:", tokens);
        }
    }
}
