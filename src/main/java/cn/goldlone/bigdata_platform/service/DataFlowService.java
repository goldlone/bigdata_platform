package cn.goldlone.bigdata_platform.service;

import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventProducer;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.dao.DataFlowDao;
import cn.goldlone.bigdata_platform.dao.DataSourceDao;
import cn.goldlone.bigdata_platform.model.*;
import cn.goldlone.bigdata_platform.utils.HDFSUtil;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Created by CN on 2018/11/24/0024 11:11 .
 */
@Service
public class DataFlowService {

  @Autowired
  private DataFlowDao dataFlowDao;

  @Autowired
  private DataSourceDao dataSourceDao;

  @Autowired
  private EventProducer eventProducer;

  @Autowired
  private HostHolder hostHolder;

  @Autowired
  private HDFSUtil hdfsUtil;

  @Value("${cluster.user.path}")
  private String clusterUserPath;

  @Value("${cluster.hive.database}")
  private String clusterHiveDatabase;


  private Result addDataFlow(DataFlow dataFlow) {

    dataFlowDao.addDataFlow(dataFlow);

    if(dataFlow.getId() == null) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "添加失败");
    }

    return ResultUtil.success();
  }

  /**
   * 添加MR任务数据流程
   * @param dataFlow
   * @return
   */
  public Result addDataFlowMR(DataFlow dataFlow) {
    Result result = null;
    // 1. 查询数据源信息，检测是否为HDFS类型数据
    DataSource dataSource = dataSourceDao.getDataSourceById(dataFlow.getSourceId());
    if (dataSource == null) {
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "数据源不存在");
    }
    if(!dataSource.getSourceType().equals("hdfs")) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "请使用HDFS类型的数据");
    }

    // 2. 确定数据源地址和结果存储位置
    String resultPath = clusterUserPath + "/" + hostHolder.getUser().getUsername()
            + "/result/" + String.valueOf(System.currentTimeMillis()) + "/";
    dataFlow.setResultPath(resultPath);

    // 3. 匹配MR算法
    switch (dataFlow.getMrName()) {
      case "wordcount": // 依次列举支持的MR算法
        // String command = "hadoop jar /app/hadoop-2.9.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.1.jar wordcount /bigdata_platform/user/gold/hdfs/1542943843448 /test/wc1";
        result = this.addDataFlow(dataFlow);
        break;
      default:
        result = ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "不支持的MR算法");
    }

    return result;
  }

  /**
   * 添加Hive数据流程
   * @param dataFlow
   * @param columns
   * @param groups
   * @return
   */
  public Result addDataFlowHive(DataFlow dataFlow, String[] columns, String[] aggregations, String[] groups) {

    // 1. 获取数据源信息，并检测是否为Hive类型
    DataSource dataSource = dataSourceDao.getDataSourceById(dataFlow.getSourceId());
    if (dataSource == null) {
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "数据源不存在");
    }
    if(!dataSource.getSourceType().equals("hive")) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "请使用Hive类型的数据");
    }

    // 2. 生成Hive结果表
    String resultTable = "res_" + dataFlow.getUserId() + "_" + String.valueOf(System.currentTimeMillis());
    dataFlow.setResultTable(resultTable);

    // 3. 拼接HQL语句
    // 3.1 拼接查询字段
    String hql = "drop table if exists " + clusterHiveDatabase + "." + resultTable
            + "; create table " + clusterHiveDatabase + "." + resultTable +" as select ";
    for(String col : groups) {
      hql += col + ",";
    }
    StringBuilder cols = new StringBuilder();
    for(int i=0; i<columns.length; i++) {
      cols.append(aggregations[i])
          .append("(")
          .append(columns[i])
          .append(")")
          .append(" as ")
          .append(aggregations[i])
          .append("_").append(columns[i])
          .append(",");
    }
    hql += cols.toString();
    hql = hql.substring(0, hql.length() - 1);
    // 3.2 拼接查询表名
    hql += " from " + clusterHiveDatabase + "." + dataSource.getHiveTable();
    // 3.3 拼接分组
    if(groups.length > 0) {
      hql += " group by " + groups[0];
      for(int i=1; i<groups.length; i++)
        hql += "," + groups[i];
    }
    hql += ";";
    dataFlow.setHiveSql(hql);

    // 4. 将数据流程保存至数据库
    return this.addDataFlow(dataFlow);
  }


  public Result startTask(Integer dataFlowId) {

    DataFlow dataFlow = dataFlowDao.getDataFlowById(dataFlowId);

    if(dataFlow == null) {
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "该数据流程不存在");
    }

    if(dataFlow.getFlowStatus().equals("Running")) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "任务正在执行，请等待结束");
    }

    eventProducer.emitEvent(new EventModel(EventType.START_DATA_FLOW)
            .setActorId(hostHolder.getUser().getId())
            .setExts("dataFlow", JSONObject.toJSONString(dataFlow)));

    return ResultUtil.success();
  }

  public List<DataFlow> getDataFlow(int userId, int offset, int limit) {

    return dataFlowDao.getDataFlow(userId, offset, limit);
  }

  public List<DataFlow> getDataFlowByStatus(int userId, String flowType, String status) {

    return dataFlowDao.getDataFlowByStatus(userId, flowType, status);
  }

  public int getDataFlowCount(int userId) {

    return dataFlowDao.getDataFlowCount(userId);
  }


  public DataFlow getDataFlowById(int dataFlowId) {

    return dataFlowDao.getDataFlowById(dataFlowId);
  }


  public Result updateDataFlow(DataFlow dataFlow) {

    if(dataFlow.getId() == null) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "缺失数据流程id");
    }

    dataFlowDao.updateDataFlow(dataFlow);

    return ResultUtil.success();
  }

  public void updateDataFlowStatus(int dataFlowId, String flowStatus) {
    dataFlowDao.updateDataFlowStatus(dataFlowId, flowStatus);
  }


  public Result deleteDataFlow(int dataFlowId) {

    // 1. 获取数据流程信息
    DataFlow dataFlow = dataFlowDao.getDataFlowById(dataFlowId);
    if(dataFlow == null) {
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "该数据流程不存在");
    }

    // 2. 检测数据源是否为当前用户所有
    if(!hostHolder.getUser().getId().equals(dataFlow.getUserId())) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "非法的越权访问");
    }

    // 3. 发送删除数据流程事件
    eventProducer.emitEvent(new EventModel(EventType.DELETE_DATA_FLOW)
            .setActorId(dataFlow.getUserId())
            .setExts("dataFlow", JSONObject.toJSONString(dataFlow)));

    return ResultUtil.success();
  }


  /**
   * 预览结果前10行
   * @param dataFlowId
   * @return
   */
  public Result previewResult(Integer dataFlowId) {

    DataFlow dataFlow = dataFlowDao.getDataFlowById(dataFlowId);

    if(dataFlow == null) {
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "数据流程不存在");
    }

    if(!dataFlow.getFlowStatus().equals("Success")) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "非Success状态不可读取结果");
    }

    List<String> list = null;
    String path = "";
    if(dataFlow.getFlowType().equals("mr")) {

      // 从hdfs中读取前n几行
      path = dataFlow.getResultPath();
    } else {

      // 从hive中读取前n行 => /user/hive/warehouse/bigdata_platform.db/{表名}
      path = "/user/hive/warehouse/bigdata_platform.db/" + dataFlow.getResultTable();
    }

    list = hdfsUtil.readNLine(path, 10);

    return ResultUtil.success("查询成功", list);
  }
}
