## 中间件的详细使用
### cache.redis
#### 应用   [redisson好的实现](https://github.com/redisson/redisson)
> * lock 单节点分布式锁
> * queue 基于redis实现分布式队列
> * bitmap 位图
> * hll HyperLogLog 在允许一定的误差下,统计不重复数据
> * bloom 布隆过滤器 判断key是否存在
> * limit 限流 1.滑动窗口限流 2.令牌桶限流 3.漏桶限流
> * geo 地图函数 实现"附件的人"
> * scan 解决keys 缺点引入scan
> * pipeline redis管道 批量传输命令到redis服务端
> * transactions redis 事务
> * pubsub  消息订阅