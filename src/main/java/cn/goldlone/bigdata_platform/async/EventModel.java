package cn.goldlone.bigdata_platform.async;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Created by CN on 2018/08/12/0012 17:30 .
 */
public class EventModel {

    /** 事件类型 */
    private EventType eventType;

    /** 触发者ID */
    private int actorId;

    /** 数据实体ID */
    private int entityId;

    /** 数据实体类型 */
    private int entityType;

    /** 事件所属者 */
    private int entityOwnerId;

    /** 扩展数据字段 */
    private Map<String, String> exts = new HashMap<String, String>();

    public EventModel() {}

    public EventModel(EventType eventType) {
        this.eventType = eventType;
    }

    public EventModel setExts(String key, String value) {
        this.exts.put(key, value);

        return this;
    }

    public EventModel setExts(Map<String, String> map) {
        this.exts.putAll(map);

        return this;
    }

    public EventType getEventType() {
        return eventType;
    }

    public EventModel setEventType(EventType eventType) {
        this.eventType = eventType;

        return this;
    }

    public int getActorId() {
        return actorId;
    }

    public EventModel setActorId(int actorId) {
        this.actorId = actorId;

        return this;
    }

    public int getEntityId() {
        return entityId;
    }

    public EventModel setEntityId(int entityId) {
        this.entityId = entityId;

        return this;
    }

    public int getEntityType() {
        return entityType;
    }

    public EventModel setEntityType(int entityType) {
        this.entityType = entityType;

        return this;
    }

    public int getEntityOwnerId() {
        return entityOwnerId;
    }

    public EventModel setEntityOwnerId(int entityOwnerId) {
        this.entityOwnerId = entityOwnerId;

        return this;
    }

    public Map<String, String> getExts() {
        return exts;
    }

    public String getExts(String key) {
        return this.exts.get(key);
    }
}
