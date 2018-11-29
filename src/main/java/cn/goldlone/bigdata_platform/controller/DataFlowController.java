package cn.goldlone.bigdata_platform.controller;

import cn.goldlone.bigdata_platform.model.*;
import cn.goldlone.bigdata_platform.service.DataFlowService;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author Created by CN on 2018/11/24/0024 12:09 .
 */
@Controller
@RequestMapping("/dataFlow")
public class DataFlowController {

  @Autowired
  private HostHolder hostHolder;

  @Autowired
  private DataFlowService dataFlowService;


  @GetMapping("/manager")
  public String manager(Model model,
                        @RequestParam(value = "p", defaultValue = "1") int pageNum) {
    User user = hostHolder.getUser();

    int pageSize = 5;
    Page page = new Page(pageNum, pageSize, dataFlowService.getDataFlowCount(user.getId()));

    List<DataFlow> list = dataFlowService.getDataFlow(user.getId(), page.getOffset(), pageSize);
    model.addAttribute("dataFlows", list);
    model.addAttribute("pages", page);

    return "dataFlowManager";
  }


  @PostMapping("/createMR")
  @ResponseBody
  public Result createDataFlowMR(Integer sourceId,
                               String dataFlowName,
                               String algorithm) {

    DataFlow dataFlow = new DataFlow();
    dataFlow.setFlowName(dataFlowName);
    dataFlow.setFlowType("mr");
    dataFlow.setSourceId(sourceId);
    dataFlow.setMrName(algorithm);
    dataFlow.setFlowStatus("Create");
    dataFlow.setUserId(hostHolder.getUser().getId());

    return dataFlowService.addDataFlowMR(dataFlow);
  }

  // http://127.0.0.1:8080/dataFlow/createHive?sourceId=3&dataFlowName=hiveTest&columns=time&aggregations=count&groups=url
  // create table res_1_1543156853869 as select url,sum(time) from 1_1_1543156486755 group by url
  @PostMapping("/createHive")
  @ResponseBody
  public Result createDataFlowHive(Integer sourceId,
                                   String dataFlowName,
                                   String[] columns,
                                   String[] aggregations,
                                   String[] groups) {

    if(columns.length != aggregations.length) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "数据字段缺失");
    }

    DataFlow dataFlow = new DataFlow();
    dataFlow.setFlowName(dataFlowName);
    dataFlow.setFlowType("hql");
    dataFlow.setSourceId(sourceId);
    dataFlow.setFlowStatus("Create");
    dataFlow.setUserId(hostHolder.getUser().getId());

    return dataFlowService.addDataFlowHive(dataFlow, columns, aggregations, groups);
  }

  // http://127.0.0.1:8080/dataFlow/start?dataFlowId=3
  // http://127.0.0.1:8080/dataFlow/start?dataFlowId=4
  @PostMapping("/start")
  @ResponseBody
  public Result startTask(Integer dataFlowId) {

    if(dataFlowId == null) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "数据流程id缺失");
    }

    return dataFlowService.startTask(dataFlowId);
  }


  // http://127.0.0.1:8080/dataFlow/delete?dataFlowId=3
  // http://127.0.0.1:8080/dataFlow/delete?dataFlowId=4
  @PostMapping("/delete")
  @ResponseBody
  public Result deleteDataFlow(Integer dataFlowId) {

    if(dataFlowId == null) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "数据流程id缺失");
    }

    return dataFlowService.deleteDataFlow(dataFlowId);
  }


  @PostMapping("/preview")
  @ResponseBody
  public Result previewResult(Integer dataFlowId) {

    if(dataFlowId == null) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "数据流程id缺失");
    }

    return dataFlowService.previewResult(dataFlowId);
  }

  @PostMapping("/selectOneById")
  @ResponseBody
  public Result selectOneById(Integer dataFlowId) {

    if(dataFlowId == null) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "数据流程id缺失");
    }

    DataFlow dataFlow = dataFlowService.getDataFlowById(dataFlowId);
    if(dataFlow == null) {
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "数据流程不存在");
    }

    return ResultUtil.success("查询成功", dataFlow);
  }

}
