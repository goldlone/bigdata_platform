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
 * 用户删除 - 事件处理器
 * @author Created by CN on 2018/11/21/0021 9:10 .
 */
@Service
public class UserDeleteHandler implements EventHandler {

  private Logger logger = LoggerFactory.getLogger(UserDeleteHandler.class);

  @Autowired
  private RemoteUtil remoteUtil;

  @Value("${cluster.user.path}")
  private String userHDFSPath;

  @Override
  public void doHandle(EventModel eventModel) {

    String username = eventModel.getExts("username");
    String command = "source /etc/profile && " +
        "hadoop fs -rm -r -f " + userHDFSPath + "/" + username;

    String msg = remoteUtil.execute(command);
    logger.info(msg);
  }

  @Override
  public List<EventType> getSupportEventTypes() {
    return Arrays.asList(EventType.USER_DELETE);
  }
}
