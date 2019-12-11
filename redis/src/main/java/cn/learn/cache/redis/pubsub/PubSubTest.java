package cn.learn.cache.redis.pubsub;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import java.util.concurrent.TimeUnit;

/**
 * 多播的消息订阅模型
 *
 * @author shaoyijiong
 * @date 2019/12/11
 */
public class PubSubTest {

  private RedisClient redisClient;
  private StatefulRedisConnection<String, String> statefulRedisConnection;
  private RedisCommands<String, String> syncCommands;
  private StatefulRedisPubSubConnection<String, String> statefulRedisPubSubConnection;
  private RedisPubSubCommands<String, String> pubSubCommands;

  /**
   * 初始化参数
   */
  private void init() {
    redisClient = RedisClient.create("redis://47.99.73.15:6379/0");
    statefulRedisConnection = redisClient.connect();
    syncCommands = statefulRedisConnection.sync();
    statefulRedisPubSubConnection = redisClient.connectPubSub();
    pubSubCommands = statefulRedisPubSubConnection.sync();

  }

  /**
   * 关闭链接
   */
  private void close() {
    statefulRedisConnection.close();
    redisClient.shutdown();
  }


  private static class Listener implements RedisPubSubListener<String, String> {

    /**
     * 订阅渠道收到消息
     */
    @Override
    public void message(String channel, String message) {
      System.out.println("message" + "---" + channel + "---" + message);
    }

    @Override
    public void message(String pattern, String channel, String message) {
      System.out.println("message" + "---" + pattern + "--" + channel + "---" + message);

    }

    /**
     * 订阅成功消息
     */
    @Override
    public void subscribed(String channel, long count) {
      System.out.println("subscribed" + "---" + channel + "---" + count);

    }

    @Override
    public void psubscribed(String pattern, long count) {
      System.out.println("psubscribed" + "---" + pattern + "---" + count);

    }

    /**
     * 取消订阅成功消息
     */
    @Override
    public void unsubscribed(String channel, long count) {
      System.out.println("unsubscribed" + "---" + channel + "---" + count);

    }

    @Override
    public void punsubscribed(String pattern, long count) {
      System.out.println("punsubscribed" + "---" + pattern + "---" + count);

    }
  }


  private static void a() throws InterruptedException {
    PubSubTest pubSubTest = new PubSubTest();
    pubSubTest.init();
    final RedisPubSubCommands<String, String> commands = pubSubTest.pubSubCommands;
    final StatefulRedisPubSubConnection<String, String> connection = pubSubTest.statefulRedisPubSubConnection;
    connection.addListener(new Listener());
    // 通过 channel 订阅
    commands.subscribe("channel1");
    TimeUnit.SECONDS.sleep(20);
    // 通过 channel 取消订阅
    commands.unsubscribe("channel1");
    pubSubTest.close();
  }

  private static void b() throws InterruptedException {
    PubSubTest pubSubTest = new PubSubTest();
    pubSubTest.init();
    final RedisPubSubCommands<String, String> commands = pubSubTest.pubSubCommands;
    final StatefulRedisPubSubConnection<String, String> connection = pubSubTest.statefulRedisPubSubConnection;
    connection.addListener(new Listener());
    // 通过 pattern 订阅
    commands.psubscribe("channel*");
    TimeUnit.SECONDS.sleep(20);
    // 通过 pattern 取消订阅
    commands.psubscribe("channel*");
    pubSubTest.close();
  }

  private static void c() {
    PubSubTest pubSubTest = new PubSubTest();
    pubSubTest.init();
    final RedisCommands<String, String> syncCommands = pubSubTest.syncCommands;
    syncCommands.publish("channel1", "hello world");
    pubSubTest.close();
  }

  public static void main(String[] args) throws InterruptedException {
    new Thread(() -> {
      try {
        b();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }).start();
    c();
  }
}
