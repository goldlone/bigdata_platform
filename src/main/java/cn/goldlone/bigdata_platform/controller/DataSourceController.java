package cn.goldlone.bigdata_platform.controller;

import cn.goldlone.bigdata_platform.model.*;
import cn.goldlone.bigdata_platform.service.DataSourceService;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author Created by CN on 2018/11/21/0021 22:57 .
 */
@Controller
@RequestMapping("/dataSource")
public class DataSourceController {


  @Autowired
  private DataSourceService dataSourceService;

  @Autowired
  private HostHolder hostHolder;


  @PostMapping("/get")
  @ResponseBody
  public Result getDataSource(Integer offset, Integer limit) {

    if(offset == null)
      offset = 0;
    if(limit == null)
      limit = Integer.MAX_VALUE;

    List<DataSource> list = dataSourceService.getDataSource(hostHolder.getUser().getId(), offset, limit);

    return ResultUtil.success("查询成功", list);
  }


  @PostMapping("/selectAllByType")
  @ResponseBody
  public Result selectAllByType(String type) {

    switch (type) {
      case "mr":
        type = "hdfs";
        break;
      case "hql":
        type = "hive";
        break;
      default:
        return ResultUtil.error(ResultCode.FAIL.getCode(), "缺失类型参数");
    }

    List<DataSource> list = dataSourceService.selectAllByType(hostHolder.getUser().getId(), type);

    return ResultUtil.success("查询成功", list);
  }

  @PostMapping("/selectOneById")
  @ResponseBody
  public Result selectOneById(Integer sourceId) {
    if(sourceId == null) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "缺失数据源id");
    }
    DataSource dataSource = dataSourceService.getDataSourceById(sourceId);

    if(dataSource != null && dataSource.getSourceType().equals("hive")) {
      return ResultUtil.success("查询成功", dataSource);
    }

    return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "该数据源不存在");
  }

  @PostMapping("/preview")
  @ResponseBody
  public Result previewTopNData(Integer sourceId, Integer n) {

    if(sourceId == null) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "数据源id参数缺失");
    }

    // 默认预览10行
    if(n == null) {
      n = 10;
    }

    return dataSourceService.previewTopNData(sourceId, n);
  }



  @PostMapping("/delete")
  @ResponseBody
  public Result deleteDataSource(Integer sourceId) {

    if(sourceId == null) {
      ResultUtil.error(ResultCode.FAIL.getCode(), "数据源id缺失");
    }

    return dataSourceService.deleteDataSource(sourceId);
  }
}
