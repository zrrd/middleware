package cn.learn.cache.redis.limit;

import java.util.HashMap;
import java.util.Map;

/**
 * 漏桶限流
 *
 * @author shaoyijiong
 * @date 2019/12/5
 */
public class FunnelRateLimiter {

  static class Funnel {

    int capacity; //漏斗容量
    float leakingRate; //漏嘴流水速率
    int leftQuota; //漏斗剩余空间
    long leakingTs; //上一次漏水时间

    public Funnel(int capacity, float leakingRate) {
      this.capacity = capacity;
      this.leakingRate = leakingRate;
      this.leftQuota = capacity;
      this.leakingTs = System.currentTimeMillis();
    }

    void makeSpace() {
      long nowTs = System.currentTimeMillis();
      long deltaTs = nowTs - leakingTs;
      // 腾出的空间
      int deltaQuota = (int) (deltaTs * leakingRate);
      //间隔时间太长，整数数字过大溢出
      if (deltaQuota < 0) {
        this.leftQuota = capacity;
        this.leakingTs = nowTs;
        return;
      }
      //腾出空间太小，最小单位是1
      if (deltaQuota < 1) {
        return;
      }
      this.leftQuota += deltaQuota;
      this.leakingTs = nowTs;
      if (this.leftQuota > this.capacity) {
        this.leftQuota = this.capacity;
      }
    }

    boolean watering(int quota) {
      // 排除漏斗
      makeSpace();
      // 剩余的空间足够
      if (this.leftQuota >= quota) {
        // 留下的空间减去进入的空间
        this.leftQuota -= quota;
        return true;
      } else {
        return false;
      }
    }
  }

  private Map<String, Funnel> funnels = new HashMap<>();

  public boolean isActionAllowed(String userId, String actionKey, int capacity, float leakingRate, int quota) {
    String key = String.format("%s:%s", userId, actionKey);
    Funnel funnel = funnels.get(key);
    if (funnel == null) {
      funnel = new Funnel(capacity, leakingRate);
      funnels.put(key, funnel);
    }
    return funnel.watering(quota);
  }
}
