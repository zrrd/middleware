package cn.learn.cache.redis.queue;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * redis 延迟队列
 *
 * @author shaoyijiong
 * @date 2019/11/27
 */
public class RedisDelayingQueue<T> {

  private RedisClient redisClient;
  private String queueKey;

  @NoArgsConstructor
  @Setter
  @Getter
  private static class TaskItem<T> {

    private String id;
    private T msg;

    static <T> TaskItem<T> create(T msg, String id) {
      TaskItem<T> item = new TaskItem<>();
      item.msg = msg;
      item.id = id;
      return item;
    }
  }

  public RedisDelayingQueue(RedisClient redisClient, String queueKey) {
    this.redisClient = redisClient;
    this.queueKey = queueKey;
  }

  /**
   * 入队
   *
   * @param msg 入队消息
   * @param millisecond 毫秒
   */
  public void delay(T msg, long millisecond) {
    // 唯一id
    String id = UUID.randomUUID().toString();
    TaskItem<T> item = TaskItem.create(msg, id);
    String s = JSON.toJSONString(item);
    try (StatefulRedisConnection<String, String> connect = redisClient.connect()) {
      RedisCommands<String, String> commands = connect.sync();
      // 塞入延迟队列
      commands.zadd(queueKey, System.currentTimeMillis() + 5000, s);
    }
  }

  /**
   * 出队
   */
  public void loop() {
    while (!Thread.interrupted()) {
      try (StatefulRedisConnection<String, String> connect = redisClient.connect()) {
        RedisCommands<String, String> commands = connect.sync();
        // 只取一条
        List<String> values = commands
            .zrangebyscore(queueKey, Range.create(0, System.currentTimeMillis()), Limit.create(0, 1));
        if (values.isEmpty()) {
          try {
            // 歇会继续
            Thread.sleep(500);
          } catch (InterruptedException e) {
            break;
          }
          continue;
        }
        String s = values.iterator().next();
        // 抢到了
        if (commands.zrem(queueKey, s) > 0) {
          // fastjson 反序列化
          TaskItem<T> task = JSON.parseObject(s, new TypeReference<TaskItem<T>>() {
          });
          this.handleMsg(task.msg);
        }
      }
    }
  }

  /**
   * 模拟处理数据
   */
  public void handleMsg(T msg) {
    System.out.println(msg);
  }

  /**
   * 测试
   */
  public static void main(String[] args) {
    RedisClient redisClient = RedisClient.create("redis://47.99.73.15:6379/0");
    RedisDelayingQueue<String> redisDelayingQueue = new RedisDelayingQueue<>(redisClient, "delayQueue");
    // 延迟入队
    redisDelayingQueue.delay("a", 5000);
    // 出队
    redisDelayingQueue.loop();
    redisClient.shutdown();
  }
}
