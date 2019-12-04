package cn.learn.cache.redis;

import com.google.common.collect.ImmutableMap;
import io.lettuce.core.KeyValue;
import io.lettuce.core.Range;
import io.lettuce.core.Range.Boundary;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.SetArgs.Builder;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

/**
 * lettuce 版本
 *
 * @author shaoyijiong
 * @date 2019/11/26
 */
@Slf4j
public class RedisTest {


  private RedisClient redisClient;
  private StatefulRedisConnection<String, String> connection;
  /**
   * 同步命令
   */
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
   * <p>章节一</p>
   * <p>Redis的基础结构 string list hash set zset</p>
   * <p>以编程语言为列 programming language -> pl </p>
   */

  /**
   * string格式操作
   */
  @Test
  public void redisString() throws InterruptedException {
    init();

    // 键值对
    // 添加单个键值对 | 返回值 OK 表示成功
    String v1 = syncCommands.set("pl", "java");
    log.info(v1);
    // 获取值 | key对应的value
    String v2 = syncCommands.get("pl");
    log.info(v2);
    // 是否存在 | key 存在的数量
    Long v3 = syncCommands.exists("pl");
    log.info(v3.toString());
    // 删除键 | key 删除的数量
    Long v4 = syncCommands.del("pl");
    log.info(v4.toString());
    // 不存在的情况 | null
    String v5 = syncCommands.get("pl");
    log.info(v5);

    log.info("--------------------------------------------------------");

    // 批量键值对
    syncCommands.set("pl1", "java");
    syncCommands.set("pl2", "js");
    // 批量获取 |  key 对应值的列表
    List<KeyValue<String, String>> v6 = syncCommands.mget("pl1", "pl2", "pl3");
    log.info(v6.toString());
    // 批量添加 | 返回值 always OK
    syncCommands.mset(ImmutableMap.of("pl1", "python", "pl2", "go"));
    List<KeyValue<String, String>> v7 = syncCommands.mget("pl1", "pl2");
    log.info(v7.toString());

    log.info("--------------------------------------------------------");

    // 过期和 set 命令扩展
    syncCommands.set("pl", "java");
    log.info(syncCommands.get("pl"));
    // 设置过期时间
    syncCommands.expire("pl", 3);
    TimeUnit.SECONDS.sleep(4);
    log.info(syncCommands.get("pl"));
    // 在创建时设置过期时间
    syncCommands.set("pl", "java", Builder.ex(3));
    TimeUnit.SECONDS.sleep(4);
    log.info(syncCommands.get("pl"));
    // 在不存在的时候创建
    syncCommands.set("pl", "java");
    log.info(syncCommands.get("pl"));
    String v8 = syncCommands.set("pl", "python", Builder.nx());
    // 返回创建失败 null
    log.info(v8);
    log.info(syncCommands.get("pl"));
    // 参数解释   可以组合一个或者多个参数
    // ex 将键的过期时间设置为 seconds 秒
    // px  将键的过期时间设置为 milliseconds 毫秒
    // nx  只在键不存在时， 才对键进行设置操作
    // xx  只在键已经存在时， 才对键进行设置操作

    log.info("---------------------------------------------------------");

    syncCommands.set("pl", "1");
    // 自增 1 | 返回自增后的数
    Long v9 = syncCommands.incr("pl");
    log.info(v9.toString());
    // 增加5 | 返回增加后的数
    Long v10 = syncCommands.incrby("pl", 5);
    log.info(v10.toString());
    // 增加-5 | 返回增加后的数
    Long v11 = syncCommands.incrby("pl", -5);
    log.info(v11.toString());

    close();
  }

  /**
   * list格式操作 链表
   */
  @Test
  public void redisList() {
    init();
    // 通过左边进右边出实现队列的操作 | 入队个数
    Long v1 = syncCommands.rpush("pl", "java", "python", "golang");
    log.info(v1.toString());
    Long v2 = syncCommands.llen("pl");
    log.info(v2.toString());
    // 右边取出一位 java
    String v3 = syncCommands.lpop("pl");
    log.info(v3);
    // python
    String v4 = syncCommands.lpop("pl");
    log.info(v4);
    // golang
    String v5 = syncCommands.lpop("pl");
    log.info(v5);
    // null
    String v6 = syncCommands.lpop("pl");
    log.info(v6);

    log.info("---------------------------------------------------------------------------------");
    // 通过左进左出实现栈的操作
    syncCommands.rpush("pl", "java", "python", "golang");
    // golang
    String v7 = syncCommands.rpop("pl");
    log.info(v7);
    // python
    String v8 = syncCommands.rpop("pl");
    log.info(v8);
    // java
    String v9 = syncCommands.rpop("pl");
    log.info(v9);
    // null
    String v10 = syncCommands.rpop("pl");
    log.info(v10);

    log.info("---------------------------------------------------------------------------------");
    // 慢操作 redis list 的底层数据结构类似链表
    syncCommands.rpush("pl", "java", "python", "golang");
    // 获取指定index的元素 | java O(n)
    String v11 = syncCommands.lindex("pl", 0);
    log.info(v11);
    // 获取所有元素 正数表示开始序号 负数表示倒数序号 | java python golang O(n)
    List<String> v12 = syncCommands.lrange("pl", 0, -1);
    log.info(v12.toString());
    // 删除num1 到 num2 以外的元素 O(n)
    syncCommands.ltrim("pl", 1, -1);
    List<String> v13 = syncCommands.lrange("pl", 0, -1);
    log.info(v13.toString());
    // 清空列表所有元素
    syncCommands.ltrim("pl", 1, 0);
    Long v14 = syncCommands.llen("pl");
    log.info(v14.toString());
    close();
  }

  /**
   * hash 操作 字典
   */
  @Test
  public void redisHash() {
    init();
    // 插入操作
    syncCommands.hset("pl", "java", "1");
    syncCommands.hset("pl", "golang", "2");
    syncCommands.hset("pl", "python", "3");
    // 批量获取值
    Map<String, String> v1 = syncCommands.hgetall("pl");
    log.info(v1.toString());
    // 获取单个值 | 1
    String v2 = syncCommands.hget("pl", "java");
    log.info(v2);
    // 获取map的长度
    Long v3 = syncCommands.hlen("pl");
    log.info(v3.toString());
    // 批量操作
    syncCommands.hmset("pl", ImmutableMap.of("java", "5", "golang", "6"));
    List<KeyValue<String, String>> v4 = syncCommands.hmget("pl", "java", "golang");
    log.info(v4.toString());
    close();
  }

  /**
   * set 操作 集合
   */
  @Test
  public void redisSet() {
    init();
    syncCommands.sadd("pl", "java");
    syncCommands.sadd("pl", "java");
    syncCommands.sadd("pl", "python");
    // 获取set中的所有元素 顺序与插入时不一致
    Set<String> v1 = syncCommands.smembers("pl");
    log.info(v1.toString());
    // 判断该元素是否在set中
    Boolean v2 = syncCommands.sismember("pl", "java");
    log.info(v2.toString());
    // 获取set的长度
    Long v3 = syncCommands.scard("pl");
    log.info(v3.toString());
    // 弹出一个
    String v4 = syncCommands.spop("pl");
    log.info(v4);
    close();
  }

  /**
   * zset 操作 有序集合
   */
  @Test
  public void redisZset() {
    init();
    // 插入数据 每一个值对应一个分数
    syncCommands.zadd("pl", 1.0, "java");
    syncCommands.zadd("pl", 3.0, "python");
    syncCommands.zadd("pl", 2.0, "golang");
    // 按照分数 由低到高  0 -1 为序列范围 | java golang python
    List<String> v1 = syncCommands.zrange("pl", 0, -1);
    log.info(v1.toString());
    // 按照分数 由高到低 | python golang java
    List<String> v2 = syncCommands.zrevrange("pl", 0, -1);
    log.info(v2.toString());
    // 获取长度
    Long v3 = syncCommands.zcard("pl");
    log.info(v3.toString());
    // 获取指定member的score值
    Double v4 = syncCommands.zscore("pl", "python");
    log.info(v4.toString());
    // 获取指定member的排名 正序  | 0
    Long v5 = syncCommands.zrank("pl", "java");
    log.info(v5.toString());
    // 获取指定member的排名 倒序 | 2
    Long v6 = syncCommands.zrevrank("pl", "java");
    log.info(v6.toString());
    // 获取指定分数区间内的member | java golang 包含边界
    List<String> v7 = syncCommands.zrangebyscore("pl", Range.create(1.0, 2.0));
    log.info(v7.toString());
    // 指定分数区间内的member 开区间 | java 下边界无边界 上边界2.0 不包含边界
    List<String> v8 = syncCommands.zrangebyscore("pl", Range.from(Boundary.unbounded(), Boundary.excluding(2.0)));
    log.info(v8.toString());
    // 获取指定分数区间内的member 与 对应的 score | java golang 包含边界
    List<ScoredValue<String>> v9 = syncCommands.zrangebyscoreWithScores("pl", Range.create(1.0, 2.0));
    log.info(v9.toString());
    syncCommands.zrem("pl", "java");
    List<String> v10 = syncCommands.zrange("pl", 0, -1);
    log.info(v10.toString());
    close();
  }
}
