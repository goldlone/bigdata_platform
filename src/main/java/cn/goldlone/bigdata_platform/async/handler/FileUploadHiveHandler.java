package cn.goldlone.bigdata_platform.async.handler;

import cn.goldlone.bigdata_platform.async.EventHandler;
import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.model.DataSource;
import cn.goldlone.bigdata_platform.model.Message;
import cn.goldlone.bigdata_platform.service.DataSourceService;
import cn.goldlone.bigdata_platform.service.MessageService;
import cn.goldlone.bigdata_platform.utils.HDFSUtil;
import cn.goldlone.bigdata_platform.utils.RemoteUtil;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @author Created by CN on 2018/11/23/0023 17:04 .
 */
@Service
public class FileUploadHiveHandler implements EventHandler {

  @Autowired
  private MessageService messageService;

  @Autowired
  private DataSourceService dataSourceService;

  @Autowired
  private RemoteUtil remoteUtil;

  @Autowired
  private HDFSUtil hdfsUtil;

  @Override
  public void doHandle(EventModel eventModel) {
    // 1. 从事件中取相关信息
    String approach = eventModel.getExts("approach");
    String filePath = eventModel.getExts("filePath");
    String command = eventModel.getExts("command");
    String dataSourceStr = eventModel.getExts("dataSource");
    DataSource dataSource = JSONObject.parseObject(dataSourceStr, DataSource.class);

    // 2. 声明消息实体
    Message message = new Message();
    message.setUserId(eventModel.getActorId());

    try {
      // 3. 上传临时文件至HDFS
      String hdfsPath = dataSource.getHdfsPath();
      dataSource.setHdfsPath(null);
      hdfsUtil.mkdirs(hdfsPath);
      hdfsUtil.upLoad(true, true, new String[]{filePath}, hdfsPath);

      // 4. 执行shell
      String result = remoteUtil.execute(command);
      System.out.println(result);

      // 当执行结果中不包含'Exception'时，认为执行成功
      if(!result.contains("Exception")) {
        // 5. 更新数据源信息
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
        message.setContent("【" + dataSource.getSourceName() + "】导入成功");
      } else {
        message.setContent("【" + dataSource.getSourceName() + "】导入语句执行出现错误：" + result);
      }
      // 删除临时文件目录
      hdfsUtil.delete(hdfsPath, true);
    } catch (Exception e) {
      e.printStackTrace();
      message.setContent("【" + dataSource.getSourceName() + "】导入失败："  + e.getMessage());
    }

    // 6. 发送消息
    messageService.addMessage(message);
  }

  @Override
  public List<EventType> getSupportEventTypes() {
    return Arrays.asList(EventType.FILE_UPLOAD_HIVE);
  }
}
