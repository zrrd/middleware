package cn.learn.cache.redis.hll;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * @author shaoyijiong
 * @date 2019/11/29
 */
public class RedisHyperLogLog {

  private RedisClient redisClient;
  private StatefulRedisConnection<String, String> connection;
  private RedisCommands<String, String> syncCommands;

  /**
   * 初始化参数
   */
  private void init() {
    redisClient = RedisClient.create("redis://47.99.73.15:6379/0");
    connection = redisClient.connect();
    syncCommands = connection.sync();
  }

  /**
   * 关闭链接
   */
  private void close() {
    connection.close();
    redisClient.shutdown();
  }

  private static void a() {
    RedisHyperLogLog hyperLogLog = new RedisHyperLogLog();
    hyperLogLog.init();
    final RedisCommands<String, String> syncCommands = hyperLogLog.syncCommands;
    // 100
    for (int i = 0; i < 100; i++) {
      //100的数量下是否有误差 | 基本上0误差
      syncCommands.pfadd("log100", String.valueOf(i));
    }
    // out 100
    System.out.println(syncCommands.pfcount("log100"));

    // 10000
    for (int i = 0; i < 10000; i++) {
      //10000的数量下是否有误差 | 存在很小的误差
      syncCommands.pfadd("log10000", String.valueOf(i));
    }
    // out 9999
    System.out.println(syncCommands.pfcount("log10000"));
    hyperLogLog.close();
  }

  public static void main(String[] args) {
    a();
  }
}
