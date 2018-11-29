package cn.goldlone.bigdata_platform.controller;

import cn.goldlone.bigdata_platform.model.User;
import cn.goldlone.bigdata_platform.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

/**
 * @author Created by CN on 2018/11/20/0020 21:27 .
 */
@Controller
public class UserController {

  @Autowired
  UserService userService;

  Logger logger = LoggerFactory.getLogger(UserController.class);

  /**
   * 用户注册 - 界面
   * @param model
   * @return
   */
  @GetMapping("/register")
  public String register(Model model) {

    return "sign-up";
  }

  /**
   * 注册
   * @param model
   * @param username
   * @param name
   * @param password
   * @param response
   * @return
   */
  @PostMapping("/register")
  public String register(Model model,
                         @RequestParam("username") String username,
                         @RequestParam("name") String name,
                         @RequestParam("password") String password,
                         HttpServletResponse response) {

    Map<String, String> map = userService.register(new User(username, password, name));
    logger.info(map.get("msg"));
    if(map.containsKey("token")) { // 注册成功
      Cookie cookie = new Cookie("token", map.get("token"));
      cookie.setPath("/");
      response.addCookie(cookie);
      return "redirect:/";
    } else {
      model.addAllAttributes(map);
    }

    return "sign-up";
  }

  /**
   * 用户登录 - 界面
   * @param callback
   * @param model
   * @return
   */
  @GetMapping("/login")
  public String registerOrLogin(@RequestParam(name = "callback", required = false) String callback,
                                Model model) {

    model.addAttribute("callback", callback);
    return "login";
  }

  @PostMapping("/login")
  public String login(Model model,
                      @RequestParam(name = "username", required = false) String username,
                      @RequestParam(name = "password", required = false) String password,
                      @RequestParam(name = "remember_me", required = false, defaultValue = "false") String rememberMe,
                      @RequestParam(name = "callback", required = false) String callback,
                      HttpServletResponse response) throws UnsupportedEncodingException {
    if(StringUtils.isEmpty(username)) {
      return "login";
    }

    Map<String, String> map = userService.loginVerify(username, password);
    logger.info(map.get("msg"));
    if(map.containsKey("token")) { // 登录成功
      Cookie cookie = new Cookie("token", map.get("token"));
      cookie.setPath("/");
      response.addCookie(cookie);

      if(StringUtils.isEmpty(callback) || !validateCallback(callback))
        return "redirect:/";

      return "redirect:"+encodeCallback(callback);
    } else {
      model.addAllAttributes(map);
    }

    return "login";
  }


  @GetMapping("/logout")
  public String logout(@CookieValue("token") String token) {
    userService.logout(token);
    return "redirect:/";
  }

  /**
   * 验证回调地址是否合法
   * @param callback
   * @return
   */
  private boolean validateCallback(String callback) {

//        if(callback.contains("http"))
//            return false;
    return true;
  }

  /**
   * 将callback的
   * @param callback
   * @return
   */
  private String encodeCallback(String callback) throws UnsupportedEncodingException {
    StringBuilder sb = new StringBuilder();
    if(!callback.contains("?"))
      return callback;
    int idx = callback.indexOf('?')+1;
    sb.append(callback.substring(0, idx));
    String p = callback.substring(idx);

    if(StringUtils.isEmpty(p))
      return sb.toString();

    String[] params = p.split("&");
    for(String param: params) {
      int i = param.indexOf('=');
      if(i == -1)
        continue;
      sb.append(param.substring(0, i))
          .append("=")
          .append(URLEncoder.encode(param.substring(i+1), "UTF-8"));
    }

    return sb.toString();
  }

}
