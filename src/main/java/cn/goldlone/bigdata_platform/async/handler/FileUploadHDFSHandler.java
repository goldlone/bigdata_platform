package cn.goldlone.bigdata_platform.async.handler;

import cn.goldlone.bigdata_platform.async.EventHandler;
import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.model.DataSource;
import cn.goldlone.bigdata_platform.model.Message;
import cn.goldlone.bigdata_platform.service.DataSourceService;
import cn.goldlone.bigdata_platform.service.MessageService;
import cn.goldlone.bigdata_platform.utils.HDFSUtil;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * 本地文件上传至HDFS
 * @author Created by CN on 2018/11/21/0021 22:38 .
 */
@Service
public class FileUploadHDFSHandler implements EventHandler {

  @Autowired
  private HDFSUtil hdfsUtil;

  @Autowired
  private DataSourceService dataSourceService;

  @Autowired
  private MessageService messageService;

  /**
   * 需要从EventModel读取'filePath', 'dataSource', 'approach'
   * @param eventModel
   */
  @Override
  public void doHandle(EventModel eventModel) {
    String dataSourceStr = eventModel.getExts("dataSource");
    DataSource dataSource = JSONObject.parseObject(dataSourceStr, DataSource.class);

    String filePath = eventModel.getExts("filePath");
    String approach = eventModel.getExts("approach");

    Message msg = null;
    try {
      System.out.println(dataSource.getHdfsPath());


      switch (approach) {
        case "overwrite": // 覆写
          hdfsUtil.delete(dataSource.getHdfsPath(), true);
        case "create": // 创建
          try {
            hdfsUtil.mkdirs(dataSource.getHdfsPath());
          } catch (Exception e) {
            System.out.println(e.getMessage());
          }
          hdfsUtil.upLoad(true, true, new String[]{filePath}, dataSource.getHdfsPath());
          break;
        case "append": // 追加
          hdfsUtil.upLoad(true, false, new String[]{filePath}, dataSource.getHdfsPath());
          break;
      }

      // 更新 数据源 信息
      if(approach.equals("create")) {
        dataSourceService.addDataSource(dataSource);
      } else {
        dataSource.setModifyData(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        dataSourceService.updateDataSource(dataSource);
      }

      // 发射一个成功的消息事件
      msg = new Message(dataSource.getUserId(), "上传至HDFS成功");

    } catch (Exception e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
      // 发射一个失败消息事件
      msg = new Message(dataSource.getUserId(), "上传至HDFS失败");
    }

    messageService.addMessage(msg);
  }

  @Override
  public List<EventType> getSupportEventTypes() {
    return Arrays.asList(EventType.FILE_UPLOAD_HDFS);
  }
}
