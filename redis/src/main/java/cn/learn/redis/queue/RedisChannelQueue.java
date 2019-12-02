package cn.learn.redis.queue;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.pubsub.RedisPubSubListener;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

/**
 * 基于Redis的发布订阅模型实现队列
 *
 * @author shaoyijiong
 * @date 2019/12/2
 */
public class RedisChannelQueue {

  private RedisClient redisClient;
  private String channel;

  public RedisChannelQueue(RedisClient redisClient, String channel) {
    this.redisClient = redisClient;
    this.channel = channel;
  }

  /**
   * 订阅消息
   */
  private void sub() {
    StatefulRedisPubSubConnection<String, String> connection = redisClient.connectPubSub();
    connection.addListener(new RedisPubSubListener<String, String>() {
      // 接受消息
      @Override
      public void message(String channel, String message) {
        System.out.println(Thread.currentThread().getName());
        System.out.println("a" + channel + message);
      }

      // 接受消息 根据channel正则匹配
      @Override
      public void message(String pattern, String channel, String message) {
        System.out.println("b" + pattern + channel + message);
      }

      // 订阅 count 订阅数量
      @Override
      public void subscribed(String channel, long count) {
        System.out.println(Thread.currentThread().getName());
        System.out.println("c" + channel + count);
      }

      //正则订阅
      @Override
      public void psubscribed(String pattern, long count) {
        System.out.println("d" + channel + count);
      }

      //退订
      @Override
      public void unsubscribed(String channel, long count) {
        System.out.println("e" + channel + count);
      }

      //正则退订
      @Override
      public void punsubscribed(String pattern, long count) {
        System.out.println("f" + channel + count);
      }
    });
    RedisPubSubCommands<String, String> sync = connection.sync();
    sync.subscribe(channel);
  }

  /**
   * 发布消息
   */
  private void pub(String message) {
    try (StatefulRedisConnection<String, String> connect = redisClient.connect()) {
      connect.sync().publish(channel, message);
    }
  }


  public static void main(String[] args) {
    RedisClient redisClient = RedisClient.create("redis://47.99.73.15:6379/0");
    RedisChannelQueue redisChannelQueue = new RedisChannelQueue(redisClient, "channel");
    redisChannelQueue.sub();
    System.out.println(Thread.currentThread().getName());
    redisChannelQueue.pub("a");
    redisChannelQueue.pub("b");
  }
}
