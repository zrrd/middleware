package cn.learn.cache.redis.pipeline;

import com.google.common.collect.Lists;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import java.util.List;
import java.util.concurrent.ExecutionException;
import lombok.extern.slf4j.Slf4j;

/**
 * @author shaoyijiong
 * @date 2019/12/10
 */
@Slf4j
public class PipelineTest {

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

  private static void a() {
    PipelineTest pipelineTest = new PipelineTest();
    pipelineTest.init();
    final RedisAsyncCommands<String, String> commands = pipelineTest.asyncCommands;
    // disable auto-flushing
    commands.setAutoFlushCommands(false);

    // 执行一系列独立的命令
    List<RedisFuture<?>> futures = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      futures.add(commands.set("key-" + i, "value-" + i));
      futures.add(commands.expire("key-" + i, 3600));
    }
    // write all commands to the transport layer
    commands.flushCommands();
    futures.forEach(f -> {
      try {
        System.out.println(f.get());
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    });
    pipelineTest.close();
  }

  public static void main(String[] args) {
    a();
  }
}
