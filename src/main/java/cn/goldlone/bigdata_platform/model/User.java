package cn.goldlone.bigdata_platform.model;

/**
 * @author Created by CN on 2018/11/20/0020 20:47 .
 */
public class User {

  // 用户id
  private Integer id;

  // 登录名
  private String username;

  // 登录密码
  private String password;

  // 昵称
  private String name;

  public User() {
  }

  public User(String username, String password, String name) {
    this.username = username;
    this.password = password;
    this.name = name;
  }

  public Integer getId() {
    return id;
  }

  public void setId(Integer id) {
    this.id = id;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return "User{" +
        "id=" + id +
        ", username='" + username + '\'' +
        ", password='" + password + '\'' +
        ", name='" + name + '\'' +
        '}';
  }
}
