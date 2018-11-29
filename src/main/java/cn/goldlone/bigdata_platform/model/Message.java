package cn.goldlone.bigdata_platform.model;

import java.util.Date;

/**
 * @author Created by CN on 2018/08/8/0008 20:29 .
 */
public class Message {

  private Integer id;

  private Integer userId;

  private String content;

  private Date createDate;

  private int hasRead;

  public Message() {
  }

  public Message(Integer userId, String content) {
    this.userId = userId;
    this.content = content;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public Integer getUserId() {
    return userId;
  }

  public void setUserId(Integer userId) {
    this.userId = userId;
  }

  public String getContent() {
    return content;
  }

  public void setContent(String content) {
    this.content = content;
  }

  public Date getCreateDate() {
    return createDate;
  }

  public void setCreateDate(Date createDate) {
    this.createDate = createDate;
  }

  public int getHasRead() {
    return hasRead;
  }

  public void setHasRead(int hasRead) {
    this.hasRead = hasRead;
  }
}
