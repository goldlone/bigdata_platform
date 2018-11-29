package cn.goldlone.bigdata_platform.service;

import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventProducer;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.dao.DataSourceDao;
import cn.goldlone.bigdata_platform.model.DataSource;
import cn.goldlone.bigdata_platform.model.DatabaseInfo;
import cn.goldlone.bigdata_platform.model.HostHolder;
import cn.goldlone.bigdata_platform.model.Result;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * @author Created by CN on 2018/11/22/0022 19:20 .
 */
@Service
public class MySQLImportService {

  @Autowired
  private HostHolder hostHolder;

  @Autowired
  private EventProducer eventProducer;

  @Value("${cluster.user.path}")
  private String clusterUserPath;

  @Value("${cluster.hive.database}")
  private String clusterHiveDatabase;

  @Autowired
  private DataSourceDao dataSourceDao;

  /**
   * 数据库导入HDFS
   * @param dataSource
   * @param databaseInfo
   * @return
   */
  public Result mysqlImportHDFS(DataSource dataSource, DatabaseInfo databaseInfo) {

    String url = "jdbc:mysql://" + databaseInfo.getAddress()
            + ":" + databaseInfo.getPort() + "/" + databaseInfo.getDatabase()
            + "?characterEncoding=" + databaseInfo.getCharset();

    dataSource.setHdfsPath(clusterUserPath + "/" + hostHolder.getUser().getUsername()
            + "/hdfs/" + String.valueOf(System.currentTimeMillis()) + "/");

    String command = String.format("source /etc/profile && " +
            "sqoop import " +
            "--connect %s " +
            "--username %s " +
            "--password %s " +
            "--table %s " +
            "--target-dir %s " +
            "--num-mappers 1", url, databaseInfo.getUsername(),
            databaseInfo.getPassword(), databaseInfo.getTable(),
            dataSource.getHdfsPath());

    eventProducer.emitEvent(new EventModel(EventType.MYSQL_IMPORT_HDFS)
            .setActorId(dataSource.getUserId())
            .setExts("dataSource", JSONObject.toJSONString(dataSource))
            .setExts("command", command).setExts("approach", "create"));

    return ResultUtil.success();
  }


  /**
   * MySQL导入HDFS，选择覆写或者追加
   * @param sourceId
   * @param approach
   * @param databaseInfo
   * @return
   */
  public Result mysqlImportHDFSPlus(Integer sourceId, String approach,
                                    DatabaseInfo databaseInfo) {

    String url = "jdbc:mysql://" + databaseInfo.getAddress()
            + ":" + databaseInfo.getPort() + "/" + databaseInfo.getDatabase()
            + "?characterEncoding=" + databaseInfo.getCharset();

    DataSource dataSource = dataSourceDao.getDataSourceById(sourceId);

    String command = null;
    if(approach.equals("overwrite")) {
      command = String.format("source /etc/profile && " +
                      "sqoop import " +
                      "--connect %s " +
                      "--username %s " +
                      "--password %s " +
                      "--table %s " +
                      "--delete-target-dir " +
                      "--direct " +
                      "--target-dir %s " +
                      "--num-mappers 1", url, databaseInfo.getUsername(),
              databaseInfo.getPassword(), databaseInfo.getTable(),
              dataSource.getHdfsPath());
    } else {
      command = String.format("source /etc/profile && " +
                      "sqoop import " +
                      "--connect %s " +
                      "--username %s " +
                      "--password %s " +
                      "--table %s " +
                      "--append " +
                      "--direct " +
                      "--target-dir %s", url, databaseInfo.getUsername(),
              databaseInfo.getPassword(), databaseInfo.getTable(),
              dataSource.getHdfsPath());
    }

    eventProducer.emitEvent(new EventModel(EventType.MYSQL_IMPORT_HDFS)
            .setActorId(dataSource.getUserId())
            .setExts("dataSource", JSONObject.toJSONString(dataSource))
            .setExts("command", command)
            .setExts("approach", approach));

    return ResultUtil.success();
  }


  /**
   * MySQL导入到Hive（创建）
   * @param dataSource
   * @param databaseInfo
   * @return
   */
  public Result mysqlImportHive(DataSource dataSource, DatabaseInfo databaseInfo) {

    String url = "jdbc:mysql://" + databaseInfo.getAddress()
            + ":" + databaseInfo.getPort() + "/" + databaseInfo.getDatabase()
            + "?characterEncoding=" + databaseInfo.getCharset();

    String currentTime = String.valueOf(System.currentTimeMillis());
    String hiveTable = String.valueOf(dataSource.getUserId()) + "_"
            + String.valueOf(dataSource.getGroupId()) + "_"
            + currentTime;
    dataSource.setHiveTable(hiveTable);

    String command = String.format("source /etc/profile && " +
                    "sqoop import " +
                    "--connect %s " +
                    "--username %s " +
                    "--password %s " +
                    "--table %s " +
                    "--direct " +
                    "--delete-target-dir " +
                    "--fields-terminated-by '\t' " +
                    "--lines-terminated-by '\n' " +
                    "--hive-import " +
                    "--hive-database %s " +
                    "--create-hive-table " +
                    "--hive-table %s " +
                    "--num-mappers 1", url, databaseInfo.getUsername(),
            databaseInfo.getPassword(), databaseInfo.getTable(),
            clusterHiveDatabase, hiveTable);

    eventProducer.emitEvent(new EventModel(EventType.MYSQL_IMPORT_HIVE)
            .setActorId(dataSource.getUserId())
            .setExts("dataSource", JSONObject.toJSONString(dataSource))
            .setExts("databaseInfo", JSONObject.toJSONString(databaseInfo))
            .setExts("command", command)
            .setExts("approach", "create"));

    return ResultUtil.success();
  }

  /**
   * MySQL导入到Hive（追加、覆写）
   * @param sourceId
   * @param approach
   * @param databaseInfo
   * @return
   */
  public Result mysqlImportHivePlus(Integer sourceId, String approach,
                                    DatabaseInfo databaseInfo) {

    String url = "jdbc:mysql://" + databaseInfo.getAddress()
            + ":" + databaseInfo.getPort() + "/" + databaseInfo.getDatabase()
            + "?characterEncoding=" + databaseInfo.getCharset();

    DataSource dataSource = dataSourceDao.getDataSourceById(sourceId);

    String command = null;
    if(approach.equals("overwrite")) {
      command = String.format("source /etc/profile && " +
                      "sqoop import " +
                      "--connect %s " +
                      "--username %s " +
                      "--password %s " +
                      "--table %s " +
                      "--direct " +
                      "--delete-target-dir " +
                      "--fields-terminated-by '\t' " +
                      "--lines-terminated-by '\n' " +
                      "--hive-import " +
                      "--hive-database %s " +
                      "--hive-table %s " +
                      "--hive-overwrite " +
                      "--num-mappers 1", url, databaseInfo.getUsername(),
              databaseInfo.getPassword(), databaseInfo.getTable(),
              clusterHiveDatabase, dataSource.getHiveTable());
    } else {
      command = String.format("source /etc/profile && " +
                      "sqoop import " +
                      "--connect %s " +
                      "--username %s " +
                      "--password %s " +
                      "--table %s " +
                      "--direct " +
                      "--delete-target-dir " +
                      "--fields-terminated-by '\t' " +
                      "--lines-terminated-by '\n' " +
                      "--hive-import " +
                      "--hive-database %s " +
                      "--hive-table %s " +
                      "--num-mappers 1", url, databaseInfo.getUsername(),
              databaseInfo.getPassword(), databaseInfo.getTable(),
              clusterHiveDatabase, dataSource.getHiveTable());
    }

    eventProducer.emitEvent(new EventModel(EventType.MYSQL_IMPORT_HIVE)
            .setActorId(dataSource.getUserId())
            .setExts("dataSource", JSONObject.toJSONString(dataSource))
            .setExts("command", command)
            .setExts("approach", approach));

    return ResultUtil.success();
  }





}
