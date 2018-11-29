package cn.goldlone.bigdata_platform.utils;

/**
 * @author Created by CN on 2018/08/12/0012 12:38 .
 */
public class RedisUtil {

  private static final String SPLIT = ":";

  private static final String BIZ_EVENT_QUEUE = "EVENT_QUEUE";

  /**
   * 异步事件key
   * @return
   */
  public static String getEventQueueKey() {

    return BIZ_EVENT_QUEUE;
  }

//
//    private static final String BIZ_LIKE = "LIKE";
//
//    private static final String BIZ_DISLIKE = "DISLIKE";

//
//    // 获取粉丝
//    private static final String BIZ_FOLLOWER = "FOLLOWER";
//
//    // 关注对象
//    private static final String BIZ_FOLLOWEE = "FOLLOWEE";
//
//    // 时间线
//    private static final String BIZ_TIMELINE = "TIMELINE";
//
//    /**
//     * 点“赞”key
//     * @param entityType
//     * @param entityId
//     * @return
//     */
//    public static String getLikeKey(int entityType, int entityId) {
//
//        return BIZ_LIKE + SPLIT + String.valueOf(entityType) + SPLIT + String.valueOf(entityId);
//    }
//
//    /**
//     * 点“踩”key
//     * @param entityType
//     * @param entityId
//     * @return
//     */
//    public static String getDislikeKey(int entityType, int entityId) {
//
//        return BIZ_DISLIKE + SPLIT + String.valueOf(entityType) + SPLIT + String.valueOf(entityId);
//    }

//
//    /**
//     * 用户粉丝key
//     * @param entityType
//     * @param entityId
//     * @return
//     */
//    public static String getFollowerKey(int entityType, int entityId) {
//        return BIZ_FOLLOWER + SPLIT + String.valueOf(entityType) + SPLIT + String.valueOf(entityId);
//    }
//
//    /**
//     * 用户关注某类实体的key
//     * @param userId
//     * @param entityType
//     * @return
//     */
//    public static String getFolloweeKey(int userId, int entityType) {
//        return BIZ_FOLLOWEE + SPLIT + String.valueOf(userId) + SPLIT + String.valueOf(entityType);
//    }
//
//
//    public static String getTimelineKey(int userId) {
//        return BIZ_TIMELINE + SPLIT + String.valueOf(userId);
//    }
}
