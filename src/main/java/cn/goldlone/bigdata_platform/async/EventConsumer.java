package cn.goldlone.bigdata_platform.async;


import cn.goldlone.bigdata_platform.utils.JedisUtil;
import cn.goldlone.bigdata_platform.utils.RedisUtil;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Created by CN on 2018/08/12/0012 17:40 .
 */
@Service
public class EventConsumer implements ApplicationContextAware, InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(EventConsumer.class);

    /** 映射表 */
    private Map<EventType, List<EventHandler>> config = new HashMap<EventType, List<EventHandler>>();

    private ApplicationContext applicationContext;

    @Autowired
    private JedisUtil jedisUtil;

    @Override
    public void afterPropertiesSet() throws Exception {
      Map<String, EventHandler> beans = applicationContext.getBeansOfType(EventHandler.class);

      if(beans != null) {
          for(Map.Entry<String, EventHandler> entry : beans.entrySet()) {
//                logger.info(entry.getKey());
//                logger.info(entry.getValue().getSupportEventTypes().toString());
//                logger.info("--------------");
              List<EventType> eventTypes = entry.getValue().getSupportEventTypes();

              for(EventType type : eventTypes) {
                  if(!config.containsKey(type))
                      config.put(type, new ArrayList<EventHandler>());

                  config.get(type).add(entry.getValue());
              }
          }
      }

      logger.info("******* 异步事件处理器映射表 ********");
      logger.info(config.toString());
      logger.info("***************");

      System.out.println("******* 异步事件处理器映射表 ********");
      for (EventType key : config.keySet()) {
        System.out.println(config.get(key));
      }
      System.out.println("***************");

      Thread thread = new Thread() {
        @Override
        public void run() {
          // 使用线程池分发事件
          ExecutorService exec = Executors.newCachedThreadPool();
          while(true) {
            String key = RedisUtil.getEventQueueKey();
            System.out.println("等待事件到来....");
            List<String> events = jedisUtil.brpop(0, key);

            for(String event : events) {
              System.out.println(event);
              // ??
              if(event.equals(key))
                continue;

              EventModel model = JSONObject.parseObject(event, EventModel.class);
              System.out.println("接收到事件：" + model.getEventType());
              exec.execute(new Consumer(model));
            }
          }
        }
      };
      thread.start();
    }


    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        this.applicationContext = applicationContext;
    }

    private class Consumer implements Runnable {

        private EventModel eventModel;

        public Consumer(EventModel eventModel) {
            this.eventModel = eventModel;
        }

        @Override
        public void run() {
            if(!config.containsKey(eventModel.getEventType())) {
                logger.error("不能识别的事件类型"+eventModel.getEventType());
                return;
            }

            for(EventHandler handler : config.get(eventModel.getEventType())) {
                handler.doHandle(eventModel);
            }
        }
    }
}