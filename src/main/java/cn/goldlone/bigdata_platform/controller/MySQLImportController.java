package cn.goldlone.bigdata_platform.controller;

import cn.goldlone.bigdata_platform.model.*;
import cn.goldlone.bigdata_platform.service.MySQLImportService;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 数据库拉取
 * @author Created by CN on 2018/11/22/0022 14:24 .
 */
@RestController
@RequestMapping("/mysql/import")
public class MySQLImportController {

  static {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }

  @PostMapping("/testConnection")
  public Result testConnection(String address,
                               String port,
                               String username,
                               String password) {
    if(StringUtils.isEmpty(address) || StringUtils.isEmpty(port) ||
            StringUtils.isEmpty(username) || StringUtils.isEmpty(password))
      return ResultUtil.error(ResultCode.FAIL.getCode(), "连接信息缺失");

    // 测试连接
    Connection conn = null;
    try {
      String url = "jdbc:mysql://"+address + ":"+port;
      conn = DriverManager.getConnection(url, username, password);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      if(conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }

    return ResultUtil.success();
  }



  @Autowired
  private HostHolder hostHolder;

  @Autowired
  private MySQLImportService mySQLImportService;

  /**
   * 数据库导入HDFS
   * @param sourceName 数据源名称
   * @param sourceType 数据源类型
   * @param groupId 数据源分组ID
   * @param address 地址
   * @param port 端口
   * @param database 数据库名
   * @param username 用户名
   * @param password 密码
   * @param table 数据表名
   * @param charset 编码方式
   * @return
   */
  @PostMapping("/hdfs")
  public Result mysqlImportHDFS(String sourceName,
                                String sourceType,
                                Integer groupId,
                                String address,
                                String port,
                                String database,
                                String username,
                                String password,
                                String table,
                                String charset) {

    DataSource dataSource = new DataSource();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dataSource.setCreateDate(sdf.format(new Date()));
    dataSource.setModifyData(sdf.format(new Date()));
    dataSource.setSourceName(sourceName);
    dataSource.setSourceType(sourceType);
    dataSource.setGroupId(groupId);
    dataSource.setUserId(hostHolder.getUser().getId());

    DatabaseInfo databaseInfo = new DatabaseInfo();
    databaseInfo.setAddress(address);
    databaseInfo.setPort(port);
    databaseInfo.setDatabase(database);
    databaseInfo.setUsername(username);
    databaseInfo.setPassword(password);
    databaseInfo.setTable(table);
    databaseInfo.setCharset(charset);

    return mySQLImportService.mysqlImportHDFS(dataSource, databaseInfo);
  }


  /**
   * MySQL追加或者覆写到HDFS
   * @param sourceId
   * @param approach
   * @param address
   * @param port
   * @param database
   * @param username
   * @param password
   * @param table
   * @param charset
   * @return
   */
  @PostMapping("/hdfsPlus")
  public Result mysqlImportHDFSPlus(Integer sourceId,
                                    String approach,
                                    String address,
                                    String port,
                                    String database,
                                    String username,
                                    String password,
                                    String table,
                                    String charset) {

    if(approach==null || (!approach.equals("append") &&
            !approach.equals("overwrite"))) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "非法的导入方式");
    }

    DatabaseInfo databaseInfo = new DatabaseInfo();
    databaseInfo.setAddress(address);
    databaseInfo.setPort(port);
    databaseInfo.setDatabase(database);
    databaseInfo.setUsername(username);
    databaseInfo.setPassword(password);
    databaseInfo.setTable(table);
    databaseInfo.setCharset(charset);

    return mySQLImportService.mysqlImportHDFSPlus(sourceId, approach, databaseInfo);
  }


  /**
   * MySQL导入到Hive（创建）
   * @param sourceName
   * @param sourceType
   * @param groupId
   * @param address
   * @param port
   * @param database
   * @param username
   * @param password
   * @param table
   * @param charset
   * @return
   */
  @PostMapping("/hive")
  public Result mysqlImportHive(String sourceName,
                                String sourceType,
                                Integer groupId,
                                String address,
                                String port,
                                String database,
                                String username,
                                String password,
                                String table,
                                String charset) {

    DataSource dataSource = new DataSource();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dataSource.setCreateDate(sdf.format(new Date()));
    dataSource.setModifyData(sdf.format(new Date()));
    dataSource.setSourceName(sourceName);
    dataSource.setSourceType(sourceType);
    dataSource.setGroupId(groupId);
    dataSource.setUserId(hostHolder.getUser().getId());

    DatabaseInfo databaseInfo = new DatabaseInfo();
    databaseInfo.setAddress(address);
    databaseInfo.setPort(port);
    databaseInfo.setDatabase(database);
    databaseInfo.setUsername(username);
    databaseInfo.setPassword(password);
    databaseInfo.setTable(table);
    databaseInfo.setCharset(charset);

    return mySQLImportService.mysqlImportHive(dataSource, databaseInfo);
  }

  @PostMapping("/hivePlus")
  public Result mysqlImportHivePlus(Integer sourceId,
                                    String approach,
                                    String address,
                                    String port,
                                    String database,
                                    String username,
                                    String password,
                                    String table,
                                    String charset) {

    if(approach==null || (!approach.equals("append") &&
            !approach.equals("overwrite"))) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "非法的导入方式");
    }

    DatabaseInfo databaseInfo = new DatabaseInfo();
    databaseInfo.setAddress(address);
    databaseInfo.setPort(port);
    databaseInfo.setDatabase(database);
    databaseInfo.setUsername(username);
    databaseInfo.setPassword(password);
    databaseInfo.setTable(table);
    databaseInfo.setCharset(charset);

    return mySQLImportService.mysqlImportHivePlus(sourceId, approach, databaseInfo);
  }

}
