package cn.learn.cache.redis.scan;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanArgs.Builder;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.HashMap;
import java.util.Map;

/**
 * @author shaoyijiong
 * @date 2019/12/9
 */
public class ScanTest {

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
    ScanTest scanTest = new ScanTest();
    scanTest.init();
    final RedisCommands<String, String> commands = scanTest.syncCommands;
    // 先向redis存入1w条数据
    int size = 10000;
    Map<String, String> map = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      map.put("key:" + i, String.valueOf(i));
    }
    commands.mset(map);
    // 游标初始为0
    String cursor = "0";
    while (true) {
      // 游标
      final ScanCursor scanCursor = ScanCursor.of(cursor);
      // limit 为100 这边的limit不是返回100条数据的意思 是限定服务器单次遍历的字典槽位数量
      // match 的作用与keys命令差不多 正则匹配
      final ScanArgs scanArgs = Builder.limit(100).match("key:*");
      final KeyScanCursor<String> scan = commands.scan(scanCursor,scanArgs);
      System.out.println(scan.getKeys());
      // 赋予新的游标 用于查询
      cursor = scan.getCursor();
      // 等价与 "0".equal(scan.getCursor())
      if (scan.isFinished()) {
        System.out.println(scan.getCursor());
        break;
      }
    }
    scanTest.close();
  }

  public static void main(String[] args) {
    a();
  }
}
