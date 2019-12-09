package cn.learn.cache.redis.geo;

import io.lettuce.core.GeoArgs;
import io.lettuce.core.GeoArgs.Builder;
import io.lettuce.core.GeoArgs.Unit;
import io.lettuce.core.GeoCoordinates;
import io.lettuce.core.GeoWithin;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.List;
import java.util.Set;

/**
 * redis geo 测试 实现附近的人
 *
 * @author shaoyijiong
 * @date 2019/12/9
 */
public class GeoTest {

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
   * 命令测试
   * <pre>
   * 位置          key 经度            维度
   * 人工智能小镇   ai  119.9629710000  30.2696590000
   * 杭州西湖风景区 xh  120.1250680000  30.2253630000
   * 杭州余杭万达   wd  119.9548920000  30.2687970000
   * </pre>
   */
  private static void a() {
    GeoTest geoTest = new GeoTest();
    geoTest.init();
    final RedisCommands<String, String> commands = geoTest.syncCommands;
    // 向zset中增加经纬度
    commands.geoadd("hz", 119.9629710000, 30.2696590000, "ai");
    commands.geoadd("hz", 120.1250680000, 30.2253630000, "xh");
    commands.geoadd("hz", 119.9548920000, 30.2687970000, "wd");
    // 计算两个元素之间的距离
    final Double geodist1 = commands.geodist("hz", "ai", "xh", Unit.km);
    final Double geodist2 = commands.geodist("hz", "ai", "wd", Unit.m);
    System.out.printf("%f,%f\n", geodist1, geodist2);
    // 获取某个地点的经纬度
    final List<GeoCoordinates> geopos = commands.geopos("hz", "ai");
    System.out.println(geopos);
    // 以给定的经纬度为中心，返回目标集合中与中心的距离不超过给定最大距离的所有位置对象。
    // 参数 WITHDIST  WITHCOORD WITHHASH ASC|DESC COUNT 1
    // 依次是 返回距离 返回经纬度 返回hash 排序方式 count
    final GeoArgs geoArgs = Builder.distance().withCoordinates().withHash().desc().withCount(2);
    // 返回 member 1km内的数据
    final List<GeoWithin<String>> georadiusbymember = commands.georadiusbymember("hz", "ai", 1, Unit.km, geoArgs);
    System.out.println(georadiusbymember);
    final Set<String> georadius = commands.georadius("hz", 119.9629710000, 30.2696590000, 1, Unit.km);
    System.out.println(georadius);
    geoTest.close();
  }

  public static void main(String[] args) {
    a();
  }
}
