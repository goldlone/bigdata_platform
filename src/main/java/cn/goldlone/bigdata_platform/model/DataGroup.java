package cn.goldlone.bigdata_platform.model;

/**
 * @author Created by CN on 2018/11/21/0021 19:53 .
 */
public class DataGroup {
  // 主键ID
  private Integer id;
  // 组名
  private String groupName;
  // 所属用户ID
  private Integer userId;

  public DataGroup() {
  }

  public DataGroup(String groupName, Integer userId) {
    this.groupName = groupName;
    this.userId = userId;
  }
  public DataGroup(Integer id, String groupName) {
    this.id = id;
    this.groupName = groupName;
  }

  public DataGroup(Integer id, String groupName, Integer userId) {
    this.id = id;
    this.groupName = groupName;
    this.userId = userId;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getGroupName() {
    return groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public Integer getUserId() {
    return userId;
  }

  public void setUserId(Integer userId) {
    this.userId = userId;
  }

  @Override
  public String toString() {
    return "DataGroup{" +
        "id=" + id +
        ", groupName='" + groupName + '\'' +
        ", userId=" + userId +
        '}';
  }
}
