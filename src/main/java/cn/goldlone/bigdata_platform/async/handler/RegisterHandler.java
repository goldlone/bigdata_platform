package cn.goldlone.bigdata_platform.async.handler;

import cn.goldlone.bigdata_platform.async.EventHandler;
import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.utils.RemoteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;

/**
 * 注册时触发事件处理器
 * 分配HDFS目录
 * @author Created by CN on 2018/11/20/0020 23:35 .
 */
@Service
public class RegisterHandler implements EventHandler {

  private Logger logger = LoggerFactory.getLogger(RegisterHandler.class);

  @Autowired
  private RemoteUtil remoteUtil;

  @Value("${cluster.user.path}")
  private String userHDFSPath;

  @Override
  public void doHandle(EventModel eventModel) {

    String username = eventModel.getExts("username");
    String command = "source /etc/profile && " +
            "hadoop fs -mkdir -p " + userHDFSPath + "/" + username + "/hdfs && " +
            "hadoop fs -mkdir -p " + userHDFSPath + "/" + username + "/hive && " +
            "hadoop fs -mkdir -p " + userHDFSPath + "/" + username + "/result";

    remoteUtil.execute(command);
    logger.info("【用户注册】HDFS初始化完毕");
  }

  @Override
  public List<EventType> getSupportEventTypes() {
    return Arrays.asList(EventType.REGISTER_INIT_HDFS);
  }
}
