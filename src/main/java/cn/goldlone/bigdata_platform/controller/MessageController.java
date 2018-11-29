package cn.goldlone.bigdata_platform.controller;

import cn.goldlone.bigdata_platform.model.*;
import cn.goldlone.bigdata_platform.service.MessageService;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author Created by CN on 2018/11/27/0027 11:10 .
 */
@Controller
@RequestMapping("/message")
public class MessageController {

  @Autowired
  private HostHolder hostHolder;

  @Autowired
  private MessageService messageService;

  @GetMapping("/manager")
  public String manager(Model model,
                        @RequestParam(value = "p", defaultValue = "1") int pageNum) {

    User user = hostHolder.getUser();

    model.containsAttribute("user");
//    model.addAttribute("user", user);

    model.addAttribute("page", "messageManager");

    int pageSize = 10;
    Page page = new Page(pageNum, pageSize, messageService.getMessageCount(user.getId()));
    List<Message> list = messageService.getMessageList(user.getId(),
            page.getOffset(), page.getPageSize());

    model.addAttribute("messages", list);
    model.addAttribute("pages", page);

    return "messageManager";
  }

  @PostMapping("/read")
  @ResponseBody
  public Result readTheMessage(Integer messageId) {

    if(messageId == null)
      return ResultUtil.error(ResultCode.FAIL.getCode(), "缺失消息id");

    messageService.updateReadMessage(messageId);

    return ResultUtil.success();
  }

}
