package cn.goldlone.bigdata_platform.service;

import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventProducer;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.dao.LoginTokenDao;
import cn.goldlone.bigdata_platform.dao.UserDao;
import cn.goldlone.bigdata_platform.model.LoginToken;
import cn.goldlone.bigdata_platform.model.User;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


/**
 * @author Created by CN on 2018/11/20/0020 20:59 .
 */
@Service
public class UserService {

  @Autowired
  private UserDao userDao;

  @Autowired
  private LoginTokenDao loginTokenDao;

  @Autowired
  private EventProducer eventProducer;

  /**
   * 注册
   * @param user
   * @return
   */
  public Map<String, String> register(User user) {
    Map<String, String> map = new HashMap<>();
    if(StringUtils.isEmpty(user.getUsername())) {
      map.put("msg", "用户名不能为空");
      return map;
    }
    if(StringUtils.isEmpty(user.getPassword())) {
      map.put("msg", "密码不能为空");
      map.put("username", user.getUsername());
      return map;
    }

    User user2 = userDao.getUserByUsername(user.getUsername());
    if(user2 != null) {
      map.put("msg", "用户名已注册");
      return map;
    }

    // 对密码进行sha256
    user.setPassword(DigestUtils.sha512Hex(user.getPassword()));

    userDao.addUser(user);

    if(user.getId() != null) {
      String token = addLoginToken(user.getId());
      map.put("token", token);
      eventProducer.emitEvent(new EventModel(EventType.REGISTER_INIT_HDFS)
          .setActorId(user.getId()).setExts("username", user.getUsername()));

    } else {
      map.put("msg", "注册失败");
    }
    return map;
  }

  /**
   * 登录
   * @param username
   * @param password
   * @return
   */
  public Map<String, String> loginVerify(String username, String password) {
    Map<String, String> map = new HashMap<>();
    if(StringUtils.isEmpty(username)) {
      map.put("msg", "用户名不能为空");
      return map;
    }
    if(StringUtils.isEmpty(username)) {
      map.put("msg", "密码不能为空");
      map.put("username", username);
      return map;
    }

    User user = userDao.getUserByUsername(username);
    if(user == null) {
      map.put("msg", "用户不存在");
      return map;
    }

    if(!user.getPassword().equals(DigestUtils.sha512Hex(password))){
      map.put("username", username);
      map.put("msg", "密码错误");
      return map;
    }

    String token = addLoginToken(user.getId());
    map.put("token", token);

    return map;
  }

  /**
   * 注销
   * @param token
   */
  public void logout(String token) {
    if(!StringUtils.isEmpty(token))
      loginTokenDao.updateStatus(token, 1);
  }

  /**
   * 根据id查询用户信息
   * @param id
   * @return
   */
  public User getUserById(int id) {
    return userDao.getUserById(id);
  }

  /**
   * 根据登录名查询用户信息
   * @param username
   * @return
   */
  public User getUserByUsername(String username) {
    return userDao.getUserByUsername(username);
  }


  /**
   * 创建token
   * @param userId
   * @return
   */
  private String addLoginToken(int userId) {
    LoginToken loginToken = new LoginToken();
    loginToken.setUserId(userId);
    Date date = new Date();
    date.setTime(3600*24*10*1000 + date.getTime());
    loginToken.setExpired(date);
    loginToken.setToken(UUID.randomUUID().toString().replaceAll("-", ""));
    loginTokenDao.addToken(loginToken);

    return loginToken.getToken();
  }

  /**
   * 根据id删除用户
   * @param userId 待删除用户id
   */
  public Map<String, String> deleteUserById(int userId) {
    Map<String, String> map = new HashMap<>();

    User user = userDao.getUserById(userId);

    if(user != null) {
      userDao.deleteUserById(userId);
      eventProducer.emitEvent(new EventModel(EventType.USER_DELETE)
          .setActorId(user.getId())
          .setExts("username", user.getUsername()));
    } else {
      map.put("msg", "用户不存在");
    }

    return map;
  }

  /**
   * 根据用户名删除用户
   * @param username 待删除用户用户名
   */
  public Map<String, String> deleteUserByUsername(String username) {
    Map<String, String> map = new HashMap<>();

    User user = userDao.getUserByUsername(username);

    if(user != null) {
      userDao.deleteUserByUsername(username);
      eventProducer.emitEvent(new EventModel(EventType.USER_DELETE)
          .setActorId(user.getId())
          .setExts("username", user.getUsername()));
    } else {
      map.put("msg", "用户不存在");
    }

    return map;
  }

}
