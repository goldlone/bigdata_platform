package cn.goldlone.bigdata_platform.async;

import java.util.List;

/**
 * @author Created by CN on 2018/08/12/0012 17:49 .
 */
public interface EventHandler {

    // 具体的处理操作
    void doHandle(EventModel eventModel);

    // 支持处理的事件类型
    List<EventType> getSupportEventTypes();
}
