package cn.goldlone.bigdata_platform.async.handler;

import cn.goldlone.bigdata_platform.async.EventHandler;
import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.dao.DataSourceDao;
import cn.goldlone.bigdata_platform.model.DataSource;
import cn.goldlone.bigdata_platform.model.Message;
import cn.goldlone.bigdata_platform.service.MessageService;
import cn.goldlone.bigdata_platform.utils.HDFSUtil;
import cn.goldlone.bigdata_platform.utils.RemoteUtil;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * @author Created by CN on 2018/11/24/0024 9:06 .
 */
@Service
public class DeleteDataSourceHandler implements EventHandler {

  @Autowired
  private DataSourceDao dataSourceDao;

  @Autowired
  private MessageService messageService;

  @Autowired
  private HDFSUtil hdfsUtil;

  @Autowired
  private RemoteUtil remoteUtil;

  @Value("${cluster.hive.database}")
  private String clusterHiveDatabase;

  @Override
  public void doHandle(EventModel eventModel) {
    Message message = new Message();
    message.setUserId(eventModel.getActorId());

    // 1. 从事件实体中获取数据源信息
    String dataSourceStr = eventModel.getExts("dataSource");
    DataSource dataSource = JSONObject.parseObject(dataSourceStr, DataSource.class);

    String type = dataSource.getSourceType();
    type = type.toLowerCase();

    // 2. 跟据数据源的类型删除存储信息
    boolean isOk = false;
    try {
      switch(type) {
        case "hdfs":
          isOk = hdfsUtil.delete(dataSource.getHdfsPath(), true);
          break;
        case "hive":
          String deleteCommand = "drop table " + clusterHiveDatabase + "." + dataSource.getHiveTable() + ";";
          String command = "source /etc/profile && hive -S -e \"" + deleteCommand + "\"";
          String result = remoteUtil.execute(command);
          System.out.println(result);
          if(!result.contains("Exception"))
            isOk = true;
          break;
      }

      // 3. 删除数据库中的数据源信息记录
      if(isOk) {
        dataSourceDao.deleteDataSource(dataSource.getId());
        message.setContent("【" + dataSource.getSourceName() + "】 删除成功");
      } else {
        message.setContent("【" + dataSource.getSourceName() + "】 删除失败");
      }
    } catch (Exception e) {
      e.printStackTrace();
      message.setContent("【" + dataSource.getSourceName() + "】 删除失败：" + e.getMessage());
    }

    messageService.addMessage(message);
  }

  @Override
  public List<EventType> getSupportEventTypes() {
    return Arrays.asList(EventType.DELETE_DATASOURCE);
  }
}
