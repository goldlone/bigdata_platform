package cn.goldlone.bigdata_platform.service;

import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventProducer;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.dao.DataSourceDao;
import cn.goldlone.bigdata_platform.model.DataSource;
import cn.goldlone.bigdata_platform.model.HostHolder;
import cn.goldlone.bigdata_platform.model.Result;
import cn.goldlone.bigdata_platform.model.ResultCode;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

/**
 * @author Created by CN on 2018/11/23/0023 10:23 .
 */
@Service
public class FileUploadService {


  @Autowired
  private HostHolder hostHolder;

  @Autowired
  private EventProducer eventProducer;

  @Autowired
  private DataSourceDao dataSourceDao;

  @Value("${cluster.user.path}")
  private String clusterUserPath;

  @Value("${cluster.hive.database}")
  private String clusterHiveDatabase;


  /**
   * 非增量导入HDFS
   * @param file
   * @param dataSource
   * @return
   */
  public Result fileUploadHDFS(MultipartFile file, DataSource dataSource) {

    // 1. 先将文件保存在临时目录中
    String tmpFilePath =  null;
    try {
      tmpFilePath = saveTempFile(dataSource.getUserId(), dataSource.getGroupId(), file);
    } catch (IOException e) {
      e.printStackTrace();
      return ResultUtil.error(ResultCode.FAIL.getCode(), "上传失败：" + e.getMessage());
    }

    dataSource.setHdfsPath(clusterUserPath + "/" + hostHolder.getUser().getUsername()
            + "/hdfs/" + String.valueOf(System.currentTimeMillis()) + "/");

    // 2. 发送上传异步事件
    eventProducer.emitEvent(new EventModel(EventType.FILE_UPLOAD_HDFS)
            .setActorId(dataSource.getUserId())
            .setExts("dataSource", JSONObject.toJSONString(dataSource))
            .setExts("filePath", tmpFilePath)
            .setExts("approach", "create"));

    return ResultUtil.success();
  }


  /**
   * 本地文件增量导入HDFS（追加、覆写）
   * @param sourceId
   * @param file
   * @param approach
   * @return
   */
  public Result fileUploadHDFSPlus(Integer sourceId, MultipartFile file, String approach) {

    // 1. 获取DataSource信息
    DataSource dataSource = dataSourceDao.getDataSourceById(sourceId);

    // 2. 先将文件保存在临时目录中
    String tmpFilePath =  null;
    try {
      tmpFilePath = saveTempFile(dataSource.getUserId(), dataSource.getGroupId(), file);
    } catch (IOException e) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "上传失败：" + e.getMessage());
    }

    // 3. 发送上传异步事件
    eventProducer.emitEvent(new EventModel(EventType.FILE_UPLOAD_HDFS)
            .setActorId(dataSource.getUserId())
            .setExts("dataSource", JSONObject.toJSONString(dataSource))
            .setExts("filePath", tmpFilePath)
            .setExts("approach", approach));

    return ResultUtil.success();
  }


  /**
   * 根据用户id、数据源组id、时间戳 生成临时文件路径
   * 并保存为临时文件
   * @param userId 根据用户id
   * @param groupId 数据源组id
   * @return 临时文件的绝对路径
   */
  private String saveTempFile(Integer userId, Integer groupId, MultipartFile file) throws IOException {
    String tmpFolder = System.getProperty("java.io.tmpdir");
    String tmpFilename = String.valueOf(userId) + "_" +
            String.valueOf(groupId) + "_" +
            String.valueOf(System.currentTimeMillis());
    String tmpFilePath = tmpFolder + tmpFilename;

    file.transferTo(new File(tmpFilePath));

    return tmpFilePath;
  }


  /**
   * 本地文件导入Hive（创建）
   * @param dataSource
   * @param file
   * @param columns
   * @return
   */
  public Result fileUploadHive(DataSource dataSource,
                               MultipartFile file,
                               String fieldTerminated,
                               String[] columns,
                               String[] dataTypes) {
    // 1. 保存临时文件
    String tempFilePath = null;
    try {
      tempFilePath = saveTempFile(dataSource.getUserId(), dataSource.getGroupId(), file);
    } catch (IOException e) {
      e.printStackTrace();
      return ResultUtil.error(ResultCode.FAIL.getCode(), "上传失败：" + e.getMessage());
    }
//    dataSource.setHdfsPath(clusterUserPath + "/" + hostHolder.getUser().getUsername()
//            + "/hive/" + String.valueOf(System.currentTimeMillis()) + "/");
    dataSource.setHdfsPath(clusterUserPath + "/" + hostHolder.getUser().getUsername()
            + "/hive/" + String.valueOf(System.currentTimeMillis()) + "/");

    // 2. 生成hive表名
    String hiveTable = String.valueOf(dataSource.getUserId()) + "_"
            + String.valueOf(dataSource.getGroupId()) + "_"
            + String.valueOf(System.currentTimeMillis());
    dataSource.setHiveTable(hiveTable);

    // 3. 拼接创建hive表语句
    String createSchema = "create table " + clusterHiveDatabase + "." + hiveTable + " ( ";
    String hiveColumn = "";
    for(int i=0; i<columns.length; i++) {
      hiveColumn += columns[i] + ",";
      createSchema += columns[i] + " " + dataTypes[i] + ",";
    }
    hiveColumn = hiveColumn.substring(0, hiveColumn.length() - 1);
    dataSource.setTableColumn(hiveColumn);
    createSchema = createSchema.substring(0, createSchema.length() - 1);
    createSchema += ") row format delimited fields terminated by '" + fieldTerminated + "';";

    // 4. 拼接将数据导入hive语句
    String loadCommand = "load data inpath '" + dataSource.getHdfsPath()
            + "' overwrite into table " + clusterHiveDatabase
            + "." + hiveTable + ";";

    // 5. 拼接shell执行命令
    String command = "source /etc/profile && hive -S -e \"" + createSchema + loadCommand + "\"";

    // 6. 启动shell事件
    eventProducer.emitEvent(new EventModel(EventType.FILE_UPLOAD_HIVE)
            .setActorId(dataSource.getUserId())
            .setExts("dataSource", JSONObject.toJSONString(dataSource))
            .setExts("filePath", tempFilePath)
            .setExts("command", command)
            .setExts("approach", "create"));

    return ResultUtil.success();
  }


  public Result fileUploadHivePlus(Integer sourceId,
                                   String approach,
                                   MultipartFile file) {
    // 1. 保存临时文件
    DataSource dataSource = dataSourceDao.getDataSourceById(sourceId);

    if(dataSource == null) {
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "该数据源不存在");
    }

    // 2. 先将文件保存在临时目录中
    String tempFilePath =  null;
    try {
      System.out.println(dataSource);
      System.out.println(file == null);
      tempFilePath = saveTempFile(dataSource.getUserId(), dataSource.getGroupId(), file);
    } catch (IOException e) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "上传失败：" + e.getMessage());
    }
    dataSource.setHdfsPath(clusterUserPath + "/" + hostHolder.getUser().getUsername()
            + "/hive/" + String.valueOf(System.currentTimeMillis()) + "/");

    // 3. 拼接将数据导入hive语句
    String loadCommand = "load data inpath '" + dataSource.getHdfsPath();
    if(approach.equals("append")) {
      loadCommand += "' ";
    } else {
      loadCommand += "' overwrite ";
    }
    loadCommand += "into table " + clusterHiveDatabase + "." + dataSource.getHiveTable() + ";";

    // 4. 拼接shell执行命令
    String command = "source /etc/profile && hive -S -e \"" + loadCommand + "\"";

    // 5. 启动shell事件
    eventProducer.emitEvent(new EventModel(EventType.FILE_UPLOAD_HIVE)
            .setActorId(dataSource.getUserId())
            .setExts("dataSource", JSONObject.toJSONString(dataSource))
            .setExts("filePath", tempFilePath)
            .setExts("command", command)
            .setExts("approach", approach));
    return ResultUtil.success();
  }

}
