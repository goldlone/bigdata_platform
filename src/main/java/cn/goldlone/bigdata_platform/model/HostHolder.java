package cn.goldlone.bigdata_platform.model;

import org.springframework.stereotype.Component;

/**
 * @author Created by CN on 2018/08/9/0009 11:51 .
 */
@Component
public class HostHolder {

    private static ThreadLocal<User> users = new ThreadLocal<User>();

    public User getUser() {
        return users.get();
    }

    public void setUsers(User user) {
        users.set(user);
    }

    public void clear() {
        users.remove();
    }
}
