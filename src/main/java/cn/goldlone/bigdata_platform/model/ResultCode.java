package cn.goldlone.bigdata_platform.model;

/**
 * @author Created by CN on 2018/08/9/0009 16:16 .
 */
public enum ResultCode {

    SUCCESS(1001, "成功"),
    FAIL(2001, "失败"),
    USER_NOT_LOGIN(2002, "用户未登录"),
    ENTITY_NOT_EXISTS(2003, "实体不存在");

    private int code;
    private String description;

    private ResultCode(int code, String description){
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return this.code;
    }

    public String getDescription() {
        return this.description;
    }
}
