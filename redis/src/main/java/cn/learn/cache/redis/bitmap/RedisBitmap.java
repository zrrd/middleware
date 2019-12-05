package cn.learn.cache.redis.bitmap;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

/**
 * 位图功能测试
 *
 * @author shaoyijiong
 * @date 2019/11/27
 */
public class RedisBitmap {


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

  /**
   * 零存整取
   */
  private static void a() {
    RedisBitmap bitTest = new RedisBitmap();
    bitTest.init();
    final RedisCommands<String, String> syncCommands = bitTest.syncCommands;

    //获取hello的二进制字节码ASCII
    final byte[] bytes = "hello".getBytes();
    List<String> list = new LinkedList<>();
    //ASCII转二进制码
    for (byte b : bytes) {
      // h -> 1101000
      final String[] split = Integer.toBinaryString(b).split("");
      // 首位填充0 h -> 1101000 -> 01101000
      for (int i = 0; i < 8 - split.length; i++) {
        list.add("0");
      }
      list.addAll(Arrays.asList(split));
    }
    System.out.println(list.toString());
    // 零存
    for (int i = list.size(); i > 0; i--) {
      final String s = list.get(list.size() - i);
      // list顺序与写入redis顺序相反
      System.out.print(s);
      syncCommands.setbit("bit1", list.size() - i, Integer.parseInt(s));
    }
    System.out.println();
    // 整取
    System.out.println(syncCommands.get("bit1"));
    bitTest.close();
  }

  /**
   * 零存零取
   */
  private static void b() {
    RedisBitmap bitTest = new RedisBitmap();
    bitTest.init();
    final RedisCommands<String, String> syncCommands = bitTest.syncCommands;
    // 存
    syncCommands.setbit("bit2", 1, 1);
    syncCommands.setbit("bit2", 2, 1);
    syncCommands.setbit("bit2", 4, 1);
    // 取
    for (int i = 0; i < 8; i++) {
      System.out.println(syncCommands.getbit("bit2", i));
    }
    bitTest.close();

  }

  /**
   * 整村零取
   */
  private static void c() {
    RedisBitmap bitTest = new RedisBitmap();
    bitTest.init();
    final RedisCommands<String, String> syncCommands = bitTest.syncCommands;
    // 存
    syncCommands.set("bit3", "h");
    // 取
    for (int i = 0; i < 8; i++) {
      System.out.println(syncCommands.getbit("bit3", i));
    }
    bitTest.close();

  }

  /**
   * 统计与查找
   */
  private static void d() {
    RedisBitmap bitTest = new RedisBitmap();
    bitTest.init();
    final RedisCommands<String, String> syncCommands = bitTest.syncCommands;
    // 存
    syncCommands.set("bit4", "hello");
    // 统计该key下所有1的数值
    System.out.println(syncCommands.bitcount("bit4"));
    // 统计该第一个字节 1 的个数start与end为字节位数,1个字节=8未
    System.out.println(syncCommands.bitcount("bit4", 0, 0));
    System.out.println(syncCommands.bitcount("bit4", 0, 1));
    // 第一个1位
    System.out.println(syncCommands.bitpos("bit4", true));
    // 第一个0位
    System.out.println(syncCommands.bitpos("bit4", false));
    // 第二个字符起 第一个1位
    System.out.println(syncCommands.bitpos("bit4", true, 1, 1));
    // 第三个字符起 第一个0位
    System.out.println(syncCommands.bitpos("bit4", false, 2, 2));
    bitTest.close();
  }

  /**
   * bitfield
   */
  private void e() {
    //bitfield 命令后面
  }

  //应用场景===========================================================

  /**
   * 统计用户签到
   */
  private static void f() {
    RedisBitmap bitTest = new RedisBitmap();
    bitTest.init();
    final RedisCommands<String, String> syncCommands = bitTest.syncCommands;
    String uid = "1";
    // 返回当前的年对应的天数
    final long day = DateUtils.getFragmentInDays(new Date(), Calendar.YEAR);
    // 年份
    final String yyyy = DateFormatUtils.format(System.currentTimeMillis(), "yyyy");
    String key = "sign:" + yyyy + ":" + uid;
    syncCommands.setbit(key, day, 1);
    // 模拟第二天签到
    syncCommands.setbit(key, day + 1, 1);
    // 模拟第三天签到
    syncCommands.setbit(key, day + 2, 1);
    // 统计该用户今年签到总数
    System.out.println(syncCommands.bitcount(key));

    // 判断今天是否签到
    System.out.println(syncCommands.getbit(key, day));
    bitTest.close();
  }

  public static void main(String[] args) {
    f();
  }

}
