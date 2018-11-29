package cn.goldlone.bigdata_platform.model;

/**
 * @author Created by CN on 2018/11/21/0021 19:56 .
 */
public class DataSource {
  // 主键ID
  private Integer id;
  // 数据源名称
  private String sourceName;
  // 数据源类型
  private String sourceType;
  // hdfs路径
  private String hdfsPath;
  // hive表
  private String hiveTable;
  // hive表结构
  private String tableColumn;
  // 所属用户
  private Integer userId;
  // 所属组
  private Integer groupId;
  // 创建时间
  private String createDate;
  // 修改时间
  private String modifyData;


  public DataSource() {
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getSourceName() {
    return sourceName;
  }

  public void setSourceName(String sourceName) {
    this.sourceName = sourceName;
  }

  public String getSourceType() {
    return sourceType;
  }

  public void setSourceType(String sourceType) {
    this.sourceType = sourceType;
  }

  public String getHdfsPath() {
    return hdfsPath;
  }

  public void setHdfsPath(String hdfsPath) {
    this.hdfsPath = hdfsPath;
  }

  public String getHiveTable() {
    return hiveTable;
  }

  public void setHiveTable(String hiveTable) {
    this.hiveTable = hiveTable;
  }

  public String getTableColumn() {
    return tableColumn;
  }

  public void setTableColumn(String tableColumn) {
    this.tableColumn = tableColumn;
  }

  public Integer getUserId() {
    return userId;
  }

  public void setUserId(Integer userId) {
    this.userId = userId;
  }

  public Integer getGroupId() {
    return groupId;
  }

  public void setGroupId(Integer groupId) {
    this.groupId = groupId;
  }

  public String getCreateDate() {
    return createDate;
  }

  public void setCreateDate(String createDate) {
    this.createDate = createDate;
  }

  public String getModifyData() {
    return modifyData;
  }

  public void setModifyData(String modifyData) {
    this.modifyData = modifyData;
  }

  @Override
  public String toString() {
    return "DataSource{" +
            "id=" + id +
            ", sourceName='" + sourceName + '\'' +
            ", sourceType='" + sourceType + '\'' +
            ", hdfsPath='" + hdfsPath + '\'' +
            ", hiveTable='" + hiveTable + '\'' +
            ", tableColumn='" + tableColumn + '\'' +
            ", userId=" + userId +
            ", groupId=" + groupId +
            ", createDate='" + createDate + '\'' +
            ", modifyData='" + modifyData + '\'' +
            '}';
  }
}
