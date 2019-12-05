package cn.learn.cache.redis.limit;

import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * redis 通过滑动窗口限流
 *
 * @author shaoyijiong
 * @date 2019/12/5
 */
public class SlidingWindowProtocol {

  private RedisClient redisClient;
  private StatefulRedisConnection<String, String> connection;
  private RedisAsyncCommands<String, String> asyncCommands;

  /**
   * 初始化参数
   */
  private void init() {
    redisClient = RedisClient.create("redis://47.99.73.15:6379/0");
    connection = redisClient.connect();
    asyncCommands = connection.async();
  }

  /**
   * 关闭链接
   */
  private void close() {
    connection.close();
    redisClient.shutdown();
  }

  /**
   * 滑动窗口限流
   *
   * @param userId 用户id
   * @param actionKey 行为
   * @param period 期限
   * @param maxCount 运行最大的流量
   * @param commands 命令
   * @return 是否触发
   */
  private boolean isActionAllowed(String userId, String actionKey, Duration period, int maxCount,
      RedisAsyncCommands<String, String> commands) {
    String key = String.format("hist:%s:%s", userId, actionKey);
    // 当前时间戳
    final long nowTs = System.currentTimeMillis();
    // 关闭自动提交 用于开启批处理
    commands.setAutoFlushCommands(false);
    // 将当时间戳作为 score 放入zset中
    commands.zadd(key, nowTs, nowTs + "");
    // 移除期限外的数据
    commands.zremrangebyscore(key, Range.create(0, nowTs - period.toMillis()));
    //获得[nowTs-period*1000,nowTs]的key数量
    final RedisFuture<Long> size = commands.zcard(key);
    // 延长有效时间
    commands.expire(key, period.getSeconds());
    commands.flushCommands();
    try {
      // 判断期限内的数量是否大于限制的数量
      return size.get() <= maxCount;
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    return true;
  }

  public static void main(String[] args) throws InterruptedException {
    SlidingWindowProtocol slidingWindowProtocol = new SlidingWindowProtocol();
    slidingWindowProtocol.init();
    final RedisAsyncCommands<String, String> asyncCommands = slidingWindowProtocol.asyncCommands;
    while (true) {
      System.out.println("go");
      final boolean block = slidingWindowProtocol.isActionAllowed("1", "list", Duration.ofSeconds(5), 3, asyncCommands);
      TimeUnit.SECONDS.sleep(1);
      if (!block) {
        System.out.println("block");
        break;
      }
    }
    slidingWindowProtocol.close();
  }
}
