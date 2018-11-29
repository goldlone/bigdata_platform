package cn.goldlone.bigdata_platform.model;

/**
 * @author Created by CN on 2018/11/22/0022 19:33 .
 */
public class DatabaseInfo {

  // 数据库IP
  private String address;
  // 端口
  private String port;
  // 数据库名称
  private String database;
  // 用户名
  private String username;
  // 密码
  private String password;
  // 数据表名称
  private String table;
  // 编码方式
  private String charset;

  public String getAddress() {
    return address;
  }

  public void setAddress(String address) {
    this.address = address;
  }

  public String getPort() {
    return port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
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

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getCharset() {
    return charset;
  }

  public void setCharset(String charset) {
    this.charset = charset;
  }

  @Override
  public String toString() {
    return "DatabaseInfo{" +
            "address='" + address + '\'' +
            ", port='" + port + '\'' +
            ", database='" + database + '\'' +
            ", username='" + username + '\'' +
            ", password='" + password + '\'' +
            ", table='" + table + '\'' +
            ", charset='" + charset + '\'' +
            '}';
  }
}
