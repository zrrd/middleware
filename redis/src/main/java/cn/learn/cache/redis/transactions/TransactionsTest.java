package cn.learn.cache.redis.transactions;

import com.google.common.collect.Lists;
import io.lettuce.core.RedisClient;
import io.lettuce.core.TransactionResult;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * @author shaoyijiong
 * @date 2019/12/10
 */
public class TransactionsTest {

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
    TransactionsTest transactionsTest = new TransactionsTest();
    transactionsTest.init();
    final RedisCommands<String, String> commands = transactionsTest.syncCommands;
    // 开启redis事务
    commands.multi();
    commands.set("a", "a");
    commands.set("b", "b");
    commands.set("c", "c");
    // 提交错误命令
    commands.incr("a");
    // 提交事务
    final TransactionResult exec = commands.exec();
    // 丢弃事务
    // commands.discard();
    System.out.println(exec);
    System.out.println(Lists.newArrayList(exec.iterator()));
    // redis事务没有原子性
    System.out.println(commands.get("a"));
    transactionsTest.close();
  }

  /**
   * 通过 redis watch 实现乐观锁
   */
  private static void b() {
    // 场景，Redis 存储了我们的账户余额数据，它是一个整数。现在有两个并发的客户端要对账户余额进行修改操作，
    // 这个修改不是一个简单的 incrby 指令，而是要对余额乘以一个倍数。
    // Redis 可没有提供 multiplyby 这样的指令。我们需要先取出余额然后在内存里乘以倍数，再将结果写回 Redis。
    TransactionsTest transactionsTest = new TransactionsTest();
    transactionsTest.init();
    final RedisCommands<String, String> commands = transactionsTest.syncCommands;
    commands.set("num", "10");
    // watch key  当事务执行时，也就是服务器收到了 exec 指令要顺序执行缓存的事务队列时，Redis 会检查关键变量自 watch 之后，是否被修改了 (包括当前事务所在的客户端)。
    // 如果关键变量被人动过了，exec 指令就会返回 null 回复告知客户端事务执行失败
    commands.watch("num");
    // 客户端1 取出数据
    final int num = Integer.parseInt(commands.get("num"));
    // 客户端2 在客户端修改前修改数据
    commands.set("num", "0");
    commands.multi();
    // 翻倍后返回
    commands.set("num", num * 2 + "");
    final TransactionResult exec = commands.exec();
    //如果数据没有变化的话 返回 OK ; 如果数据变化 返回空串
    System.out.println(Lists.newArrayList(exec.iterator()));
  }

  public static void main(String[] args) {
    b();
  }
}
