package cn.goldlone.bigdata_platform.controller;

import cn.goldlone.bigdata_platform.model.*;
import cn.goldlone.bigdata_platform.service.DataFlowService;
import cn.goldlone.bigdata_platform.service.DataShowService;
import cn.goldlone.bigdata_platform.service.DataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;

/**
 * 数据可视化
 * @author Created by CN on 2018/11/28/0028 14:12 .
 */
@Controller
@RequestMapping("/dataShow")
public class DataShowController {

  @Autowired
  private DataSourceService dataSourceService;

  @Autowired
  private DataFlowService dataFlowService;

  @Autowired
  private DataShowService dataShowService;

  @Autowired
  private HostHolder hostHolder;

  @GetMapping("/manager")
  public String manager(Model model) {

    model.addAttribute("page", "dataShow");

    User user = hostHolder.getUser();
    List<DataSource> dataSourceList = dataSourceService.selectAllByType(user.getId(), "hive");
    List<DataFlow> dataFlowList = dataFlowService.getDataFlowByStatus(user.getId(), "hql", "Success");

    model.addAttribute("dataSourceList", dataSourceList);
    model.addAttribute("dataFlowList", dataFlowList);

    return "dataShow";
  }


  @PostMapping("/getDataSourceDataFromHive")
  @ResponseBody
  public Result getDataSourceDataFromHive(Integer dataSourceId) {

    return dataShowService.getDataSourceDataFromHive(dataSourceId);
  }



  @PostMapping("/getDataFlowResultDataFromHive")
  @ResponseBody
  public Result getDataFlowResultDataFromHive(Integer dataFlowId) {

    return dataShowService.getDataFlowResultDataFromHive(dataFlowId);
  }


  @PostMapping("/getDataFlowInfoFromHive")
  @ResponseBody
  public Result getDataFlowInfoFromHive(Integer dataFlowId) {

    return dataShowService.getDataFlowInfoFromHive(dataFlowId);
  }

}
