package cn.goldlone.bigdata_platform.async.handler;

import cn.goldlone.bigdata_platform.async.EventHandler;
import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.model.DataFlow;
import cn.goldlone.bigdata_platform.model.DataSource;
import cn.goldlone.bigdata_platform.model.Message;
import cn.goldlone.bigdata_platform.service.DataFlowService;
import cn.goldlone.bigdata_platform.service.DataSourceService;
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
 * @author Created by CN on 2018/11/26/0026 9:10 .
 */
@Service
public class StartDataFlowHandler implements EventHandler {

  @Autowired
  private DataFlowService dataFlowService;

  @Autowired
  private DataSourceService dataSourceService;

  @Autowired
  private MessageService messageService;

  @Autowired
  private RemoteUtil remoteUtil;

  @Autowired
  private HDFSUtil hdfsUtil;

  @Value("${cluster.hive.database}")
  private String clusterHiveDatabase;

  @Override
  public void doHandle(EventModel eventModel) {
    Message message = new Message();
    message.setUserId(eventModel.getActorId());

    String dataFlowStr = eventModel.getExts("dataFlow");
    DataFlow dataFlow = JSONObject.parseObject(dataFlowStr, DataFlow.class);

    if(dataFlow.getFlowStatus().equals("Running")) {
      message.setContent("【"+dataFlow.getFlowName()+"】任务正在执行中，请勿重复提交");
      messageService.addMessage(message);
      return;
    }

    DataSource dataSource = dataSourceService.getDataSourceById(dataFlow.getSourceId());
    if(dataSource != null) {
      switch (dataFlow.getFlowType()) {
        case "hql": {
          // 运行HQL语句
          String hql = dataFlow.getHiveSql();
          dataFlowService.updateDataFlowStatus(dataFlow.getId(), "Running");
          if(dataFlow.getFlowStatus().equals("Success")) {
            hql = "drop table if exists " + clusterHiveDatabase + "." + dataFlow.getResultTable() + ";" + hql;
          }
          String command = "source /etc/profile && hive -S -e \"" + hql + "\"";
          String result = remoteUtil.execute(command);
          System.out.println(result);

          if (!result.contains("Exception")) {
            dataFlowService.updateDataFlowStatus(dataFlow.getId(), "Success");
            message.setContent("【" + dataFlow.getFlowName() + "】 执行成功");
          } else {
            dataFlowService.updateDataFlowStatus(dataFlow.getId(), "Fail");
            message.setContent("【" + dataFlow.getFlowName() + "】 执行失败：" + result);
          }
          break;
        }
        case "mr": {
          // 先清空结果路径
          if (!dataFlow.getFlowStatus().equals("Create")) {
            hdfsUtil.delete(dataFlow.getResultPath(), true);
          }
          // 提交MR任务
          String command = null;
          switch (dataFlow.getMrName()) {
            case "wordcount":
              command = "source /etc/profile && hadoop jar "
                      + "/app/hadoop-2.9.1/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.9.1.jar "
                      + "wordcount " + dataSource.getHdfsPath()
                      + " " + dataFlow.getResultPath();
              break;
          }
          if (command == null) {
            message.setContent("【" + dataFlow.getFlowName() + "】未匹配的MR算法");
          } else {
            dataFlowService.updateDataFlowStatus(dataFlow.getId(), "Running");
            String result = remoteUtil.execute(command);
            System.out.println(result);
            if (result.contains("completed successfully")) {
              dataFlowService.updateDataFlowStatus(dataFlow.getId(), "Success");
              message.setContent("【" + dataFlow.getFlowName() + "】 执行成功");
            } else {
              dataFlowService.updateDataFlowStatus(dataFlow.getId(), "Fail");
              message.setContent("【" + dataFlow.getFlowName() + "】 执行失败：" + result);
            }
          }
          break;
        }
        default:
          message.setContent("【" + dataFlow.getFlowName() + "】未匹配数据流程类型");
          break;
      }
    } else {
      message.setContent("【"+dataFlow.getFlowName()+"】数据源不存在");
    }

    messageService.addMessage(message);
  }

  @Override
  public List<EventType> getSupportEventTypes() {
    return Arrays.asList(EventType.START_DATA_FLOW);
  }
}
