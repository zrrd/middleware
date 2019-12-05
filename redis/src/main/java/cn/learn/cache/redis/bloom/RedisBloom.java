package cn.learn.cache.redis.bloom;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.dynamic.Commands;
import io.lettuce.core.dynamic.RedisCommandFactory;
import io.lettuce.core.dynamic.annotation.CommandNaming;
import io.lettuce.core.dynamic.annotation.CommandNaming.Strategy;
import java.util.List;

/**
 * redis 的bloom过滤器
 *
 * @author shaoyijiong
 * @date 2019/12/5
 */
public class RedisBloom {

  private RedisClient redisClient;
  private StatefulRedisConnection<String, String> connection;
  private BloomCommands bloomCommands;

  /**
   * 实现自己的命令 Strategy.DOT 驼峰用
   */
  @CommandNaming(strategy = Strategy.DOT)
  interface BloomCommands extends Commands {

    /**
     * 将值放入对应布隆过滤器的key中
     *
     * @param key redis key
     * @param value redis value
     */
    void bfAdd(String key, String value);

    /**
     * 布隆过滤器中是否存在
     *
     * @param key redis key
     * @param value redis value
     * @return 是否存在
     */
    Boolean bfExists(String key, String value);

    /**
     * 批量 bfAdd
     */
    void bfMadd(String key, String... values);

    /**
     * 批量 bfExists
     */
    List<Boolean> bfMexists(String key, String... values);

    /**
     * 布隆过滤器配置(在bf.add前使用 如果该key已经存在会报错)
     */
    void bfReserve(String key, Double errorRate, Integer initialSize);
  }

  /**
   * 初始化参数
   */
  private void init() {
    redisClient = RedisClient.create("redis://47.99.73.15:6379/0");
    connection = redisClient.connect();
    RedisCommandFactory factory = new RedisCommandFactory(redisClient.connect());
    bloomCommands = factory.getCommands(BloomCommands.class);
  }

  /**
   * 关闭链接
   */
  private void close() {
    connection.close();
    redisClient.shutdown();
  }


  private static void a() {
    RedisBloom redisBloom = new RedisBloom();
    redisBloom.init();
    final BloomCommands commands = redisBloom.bloomCommands;
    commands.bfAdd("b", "a");
    System.out.println(commands.bfExists("b", "a"));
    commands.bfMadd("c", "a", "b");
    System.out.println(commands.bfMexists("c", "a", "b", "c"));
    commands.bfReserve("d", 0.001, 10000);
    redisBloom.close();
  }

  private static void b() {
    RedisBloom redisBloom = new RedisBloom();
    redisBloom.init();
    final BloomCommands commands = redisBloom.bloomCommands;
    // 数据量 1w
    int size = 10000;
    String key = "user";
    // key user , 错误率 0.001 , 大小 size + 1/2 size 冗余
    commands.bfReserve(key, 0.01, size + size / 2);
    // 插入
    for (int i = 0; i < size; i++) {
      commands.bfAdd(key, "u" + i);
    }
    int count = 0;
    // 统计
    for (int i = 0; i < size; i++) {
      // 从一半开始统计
      if (commands.bfExists(key, "u" + (size / 2 + i))) {
        count++;
      }
    }
    // 基本上在5000 误判率很低
    System.out.println(count);
    redisBloom.close();
  }

  public static void main(String[] args) {
    b();
  }

}
