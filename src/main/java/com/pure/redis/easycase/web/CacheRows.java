package com.pure.redis.easycase.web;

import com.google.gson.Gson;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Set;

public class CacheRows extends Thread {
    private Jedis jedis;
    private boolean quit;

    public CacheRows(Jedis jedis){
        this.jedis = jedis;
        this.jedis.select(14);
    }

    public void quit(){
        this.quit =true;
    }


    @Override
    public void run() {
        while (!quit){
            Gson gson = new Gson();
            while (!quit){
                //获取需要缓存的数据行
                Set<Tuple> range = jedis.zrangeWithScores("schedule:", 0, 0);
                Tuple next = range.size() > 0 ? range.iterator().next() : null;
                long now = System.currentTimeMillis() / 1000;
                if (next == null || next.getScore() > now){
                    try {
                        Thread.sleep(50);
                    }catch(InterruptedException ie){
                        Thread.currentThread().interrupt();
                    }
                    continue;
                }

                String rowId = next.getElement();
                double delay = jedis.zscore("delay:", rowId);
                if (delay <= 0) {
                    jedis.zrem("delay:", rowId);
                    jedis.zrem("schedule:", rowId);
                    jedis.del("inv:" + rowId);
                    continue;
                }

                Inventory row = Inventory.get(rowId);
                jedis.zadd("schedule:", now + delay, rowId);
                jedis.set("inv:" + rowId, gson.toJson(row));
            }
        }
    }
}
