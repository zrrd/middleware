package cn.learn.cache.redis.queue;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * redis 实现阻塞队列
 *
 * @author shaoyijiong
 * @date 2019/11/27
 */
public class RedisBlockingQueue<T> {

  private RedisClient redisClient;
  private String queueKey;

  public RedisBlockingQueue(RedisClient redisClient, String queueKey) {
    this.redisClient = redisClient;
    this.queueKey = queueKey;
  }

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

  public T take(Duration timeout) {
    try (StatefulRedisConnection<String, String> connect = redisClient.connect()) {
      final RedisCommands<String, String> commands = connect.sync();
      // 阻塞读 右边出
      final KeyValue<String, String> pop = commands.brpop(timeout.getSeconds(), queueKey);
      final String s = pop.getValue();
      if (s == null) {
        return null;
      } else {
        final TaskItem<T> item = JSON.parseObject(s, new TypeReference<TaskItem<T>>() {
        });
        return item.getMsg();
      }
    }
  }

  public void put(T msg) {
    try (StatefulRedisConnection<String, String> connect = redisClient.connect()) {
      final RedisCommands<String, String> commands = connect.sync();
      final TaskItem<T> item = TaskItem.create(msg, UUID.randomUUID().toString());
      // 左边进
      commands.lpush(queueKey, JSON.toJSONString(item));
    }
  }

  public static void main(String[] args) throws InterruptedException {
    RedisClient redisClient = RedisClient.create("redis://47.99.73.15:6379/0");
    RedisBlockingQueue<String> redisBlockingQueue = new RedisBlockingQueue<>(redisClient, "blockQueue");
    // 读取数据 超时时间1天
    new Thread(() -> {
      // 只要有数据就出队 否则等待最长1天时间 (等待时间过长可能导致系统杀死连接)
      System.out.println(redisBlockingQueue.take(Duration.ofDays(1L)));
    }).start();
    TimeUnit.SECONDS.sleep(5);
    System.out.println("开始入队");
    redisBlockingQueue.put("go");
  }
}
