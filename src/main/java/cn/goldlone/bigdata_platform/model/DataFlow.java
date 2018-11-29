package cn.goldlone.bigdata_platform.model;

/**
 * @author Created by CN on 2018/11/21/0021 20:08 .
 */
public class DataFlow {

  // 主键id
  private Integer id;

  // 数据流程名
  private String flowName;

  // 数据源id
  private Integer sourceId;

  // 数据流程类型（MR、HQL）
  private String flowType;

  // MR任务名
  private String mrName;

  // HQL语句
  private String hiveSql;

  // 数据流程执行状态()
  private String flowStatus;

  // Hive执行结果表
  private String resultTable;

  // MR执行结果位置
  private String resultPath;

  // 用户id
  private Integer userId;


  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getFlowName() {
    return flowName;
  }

  public void setFlowName(String flowName) {
    this.flowName = flowName;
  }

  public Integer getSourceId() {
    return sourceId;
  }

  public void setSourceId(Integer sourceId) {
    this.sourceId = sourceId;
  }

  public String getFlowType() {
    return flowType;
  }

  public void setFlowType(String flowType) {
    this.flowType = flowType;
  }

  public String getMrName() {
    return mrName;
  }

  public void setMrName(String mrName) {
    this.mrName = mrName;
  }

  public String getHiveSql() {
    return hiveSql;
  }

  public void setHiveSql(String hiveSql) {
    this.hiveSql = hiveSql;
  }

  public String getFlowStatus() {
    return flowStatus;
  }

  public void setFlowStatus(String flowStatus) {
    this.flowStatus = flowStatus;
  }

  public String getResultTable() {
    return resultTable;
  }

  public void setResultTable(String resultTable) {
    this.resultTable = resultTable;
  }

  public String getResultPath() {
    return resultPath;
  }

  public void setResultPath(String resultPath) {
    this.resultPath = resultPath;
  }

  public Integer getUserId() {
    return userId;
  }

  public void setUserId(Integer userId) {
    this.userId = userId;
  }
}
