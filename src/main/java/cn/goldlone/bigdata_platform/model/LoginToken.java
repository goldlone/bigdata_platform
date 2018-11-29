package cn.goldlone.bigdata_platform.model;

import java.util.Date;

/**
 * @author Created by CN on 2018/08/9/0009 10:43 .
 */
public class LoginToken {

    private Integer id;

    private Integer userId;

    private String token;

    private Date expired;

    private Integer status;

    public LoginToken() {
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public Date getExpired() {
        return expired;
    }

    public void setExpired(Date expired) {
        this.expired = expired;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "LoginToken{" +
                "id=" + id +
                ", userId=" + userId +
                ", token='" + token + '\'' +
                ", expired=" + expired +
                ", status=" + status +
                '}';
    }
}
