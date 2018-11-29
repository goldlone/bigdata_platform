package cn.goldlone.bigdata_platform.controller;

import cn.goldlone.bigdata_platform.model.*;
import cn.goldlone.bigdata_platform.service.DataGroupService;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * @author Created by CN on 2018/11/26/0026 14:24 .
 */
@Controller
@RequestMapping("/dataGroup")
public class DataGroupController {

  @Autowired
  private HostHolder hostHolder;

  @Autowired
  private DataGroupService dataGroupService;

  /**
   * 数据集分组管理界面
   * @param model
   * @return
   */
  @GetMapping("/manager")
  public String manager(Model model) {

    model.addAttribute("page", "dataGroupManager");

    User user = hostHolder.getUser();

    if(user != null) {
      model.addAttribute("user", user);
    }

    List<DataGroup> list = dataGroupService.getDataGroup(user.getId(), 0, Integer.MAX_VALUE);

    model.addAttribute("dataGroups", list);

    return "dataGroupManager";
  }


  /**
   * 添加分组
   * @param dataGroupName
   * @return
   */
  @PostMapping("/add")
  @ResponseBody
  public Result add(String dataGroupName) {

    DataGroup dataGroup = new DataGroup(dataGroupName, hostHolder.getUser().getId());

    return dataGroupService.addDataGroup(dataGroup);
  }


  /**
   * 修改分组名称
   * @param dataGroupId
   * @param dataGroupName
   * @return
   */
  @PostMapping("/update")
  @ResponseBody
  public Result update(Integer dataGroupId, String dataGroupName) {

    DataGroup dataGroup = new DataGroup();
    dataGroup.setId(dataGroupId);
    dataGroup.setGroupName(dataGroupName);

    return dataGroupService.updateDataGroup(dataGroup);
  }


  /**
   * 删除
   * @param dataGroupId
   * @return
   */
  @PostMapping("delete")
  @ResponseBody
  public Result delete(Integer dataGroupId) {

    if(dataGroupId == null) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "数据源分组id缺失");
    }

    return dataGroupService.deleteDataGroup(dataGroupId);
  }


}
