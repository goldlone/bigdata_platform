package cn.goldlone.bigdata_platform.async.handler;

import cn.goldlone.bigdata_platform.async.EventHandler;
import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.model.DataSource;
import cn.goldlone.bigdata_platform.model.DatabaseInfo;
import cn.goldlone.bigdata_platform.model.Message;
import cn.goldlone.bigdata_platform.service.DataSourceService;
import cn.goldlone.bigdata_platform.service.MessageService;
import cn.goldlone.bigdata_platform.utils.DBUtil;
import cn.goldlone.bigdata_platform.utils.RemoteUtil;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author Created by CN on 2018/11/22/0022 19:08 .
 */
@Service
public class MySQLImportHDFSHandler implements EventHandler {

  @Autowired
  private RemoteUtil remoteUtil;

  @Autowired
  private DataSourceService dataSourceService;

  @Autowired
  private MessageService messageService;

  @Override
  public void doHandle(EventModel eventModel) {
    Message msg = new Message();
    msg.setUserId(eventModel.getActorId());
    String command = eventModel.getExts("command");
    String approach = eventModel.getExts("approach");
    DataSource dataSource = JSONObject.parseObject(eventModel.getExts("dataSource"), DataSource.class);

    try {
      String result = remoteUtil.execute(command);

      System.out.println("========= 【MySQLImportHDFSHandler】 执行结果 =========");
      System.out.println(result);

      if(result.contains("completed successfully")) {
        if(eventModel.getEventType() == EventType.MYSQL_IMPORT_HIVE && approach.equals("create")) {
          DatabaseInfo databaseInfo = JSONObject.parseObject(eventModel.getExts("databaseInfo"), DatabaseInfo.class);
          String url = "jdbc:mysql://" + databaseInfo.getAddress()
                  + ":" + databaseInfo.getPort() + "/" + databaseInfo.getDatabase()
                  + "?characterEncoding=" + databaseInfo.getCharset();

          DBUtil dbUtil = new DBUtil(url, databaseInfo.getUsername(), databaseInfo.getPassword());
          List<String> list = dbUtil.getTableInfo(databaseInfo.getTable());
          StringBuilder cols = new StringBuilder();
          for(String str: list) {
            cols.append(str.split(",")[0]).append(",");
          }
          cols.deleteCharAt(cols.length() - 1);
          dataSource.setTableColumn(cols.toString());
        }
        // 导入成功
        switch (approach) {
          case "create":
            dataSourceService.addDataSource(dataSource);
            break;
          case "append":
          case "overwrite":
            dataSource.setModifyData(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            dataSourceService.updateDataSource(dataSource);
            break;
        }

        if(eventModel.getEventType() == EventType.MYSQL_IMPORT_HDFS) {
          msg.setContent("【"+dataSource.getSourceName()+"】MySQL导入HDFS成功");
        } else {
          msg.setContent("【"+dataSource.getSourceName()+"】MySQL导入Hive成功");
        }

      } else {
        // 导入失败
        if(eventModel.getEventType() == EventType.MYSQL_IMPORT_HDFS) {
          msg.setContent("【"+dataSource.getSourceName()+"】MySQL导入HDFS失败");
        } else {
          msg.setContent("【"+dataSource.getSourceName()+"】MySQL导入Hive失败");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      msg = new Message(eventModel.getActorId(), "【"+dataSource.getSourceName()+"】导入失败："+e.getMessage());
    }

    messageService.addMessage(msg);
  }

  @Override
  public List<EventType> getSupportEventTypes() {
    return Arrays.asList(EventType.MYSQL_IMPORT_HDFS, EventType.MYSQL_IMPORT_HIVE);
  }
}
