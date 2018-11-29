package cn.goldlone.bigdata_platform.interceptor;

import cn.goldlone.bigdata_platform.dao.LoginTokenDao;
import cn.goldlone.bigdata_platform.dao.UserDao;
import cn.goldlone.bigdata_platform.model.HostHolder;
import cn.goldlone.bigdata_platform.model.LoginToken;
import cn.goldlone.bigdata_platform.model.User;
import cn.goldlone.bigdata_platform.service.MessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Date;

/**
 * @author Created by CN on 2018/08/9/0009 11:41 .
 */
@Component
public class PassportInterceptor implements HandlerInterceptor {

  private Logger logger = LoggerFactory.getLogger(PassportInterceptor.class);

    @Autowired
    LoginTokenDao loginTokenDao;

    @Autowired
    UserDao userDao;

    @Autowired
    HostHolder hostHolder;

  @Autowired
  private MessageService messageService;

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response,
                             Object handler)
            throws Exception {

      String token = null;
      if(request.getCookies() != null) {
        for(Cookie cookie: request.getCookies()) {
          if(cookie.getName().equals("token")) {
            token = cookie.getValue();
            break;
          }
        }
      }

      if(token != null) {
        LoginToken loginToken = loginTokenDao.getLoginTokenByToken(token);
        if(loginToken == null ||
                loginToken.getExpired().before(new Date()) ||
                loginToken.getStatus() == 1) {
          return true;
        }

        User user = userDao.getUserById(loginToken.getUserId());
        hostHolder.setUsers(user);
      }
      return true;
    }

    @Override
    public void postHandle(HttpServletRequest request,
                           HttpServletResponse response,
                           Object handler,
                           ModelAndView modelAndView)
            throws Exception {

      if(modelAndView != null) {
        User user = hostHolder.getUser();
        modelAndView.addObject("user", user);
        if(user != null) {
          modelAndView.addObject("messageUnread", messageService.getUnreadMessageCount(hostHolder.getUser().getId()));
        }
      }
    }

    @Override
    public void afterCompletion(HttpServletRequest request,
                                HttpServletResponse response,
                                Object handler,
                                Exception ex)
            throws Exception {

      hostHolder.clear();
    }
}
