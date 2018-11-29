package cn.goldlone.bigdata_platform.dao;

import cn.goldlone.bigdata_platform.model.Message;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @author Created by CN on 2018/08/8/0008 20:37 .
 */
@Mapper
public interface MessageDao {

  String TABLE_NAME = " message ";
  String INSERT_FIELD = "  user_id, content, create_date, has_read ";
  String SELECT_FIELD = " id, " + INSERT_FIELD;


  /**
   * 创建消息
   * @param message
   */
  @Select({"insert into ", TABLE_NAME, "(", INSERT_FIELD, ")",
          "values(#{userId}, #{content}, now(), 0)"})
  void addMessage(Message message);

  /**
   * 查询历史消息
   * @param userId
   * @param offset
   * @param limit
   * @return
   */
  @Select({"select ", SELECT_FIELD, " from ", TABLE_NAME,
          " where user_id=#{userId} order by create_date desc" +
          " limit #{offset}, #{limit}"})
  List<Message> getMessageList(@Param("userId") int userId,
                               @Param("offset") int offset,
                               @Param("limit") int limit);

  @Select({"select count(1) from ", TABLE_NAME,
          " where user_id=#{userId}"})
  int getMessageCount(int userId);

  /**
   * 查询未读消息
   * @param userId
   * @return
   */
  @Select({"select count(*) from ", TABLE_NAME,
          " where user_id=#{userId} and has_read=0"})
  int getUnreadMessageCount(@Param("userId") int userId);

  /**
   * 标记消息已读
   * @param id
   */
  @Select({"update ", TABLE_NAME, " set has_read=1 ",
          " where id=#{id} and has_read=0"})
  void updateReadMessage(@Param("id") int id);
}
