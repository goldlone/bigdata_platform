package cn.goldlone.bigdata_platform.dao;

import cn.goldlone.bigdata_platform.model.DataFlow;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author Created by CN on 2018/11/24/0024 10:42 .
 */
@Mapper
public interface DataFlowDao {


  /**
   * 查询数据流信息
   * @param userId
   * @param offset
   * @param limit
   * @return
   */
  List<DataFlow> getDataFlow(@Param("userId") int userId,
                             @Param("offset") int offset,
                             @Param("limit") int limit);

  /**
   *
   * @param userId
   * @param status
   * @return
   */
  List<DataFlow> getDataFlowByStatus(@Param("userId") int userId,
                                     @Param("flowType") String flowType,
                                     @Param("status") String status);

  /**
   * 总个数
   * @param userId
   * @return
   */
  int getDataFlowCount(int userId);

  /**
   * 根据id查询数据流程信息
   * @param dataFlowId
   * @return
   */
  DataFlow getDataFlowById(int dataFlowId);


  /**
   * 添加数据流程
   * @param dataFlow
   */
  void addDataFlow(DataFlow dataFlow);


  /**
   * 更新数据流程
   * @param dataFlow
   */
  void updateDataFlow(DataFlow dataFlow);


  /**
   * 更新数据流程状态
   * @param dataFlowId
   * @param flowStatus
   */
  void updateDataFlowStatus(@Param("dataFlowId") int dataFlowId,
                            @Param("flowStatus") String flowStatus);


  /**
   * 删除数据流程
   * @param dataFlowId
   */
  void deleteDataFlow(int dataFlowId);

}
