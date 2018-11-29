package cn.goldlone.bigdata_platform.service;

import cn.goldlone.bigdata_platform.dao.DataGroupDao;
import cn.goldlone.bigdata_platform.model.DataGroup;
import cn.goldlone.bigdata_platform.model.Result;
import cn.goldlone.bigdata_platform.model.ResultCode;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.util.HtmlUtils;

import java.util.List;

/**
 * @author Created by CN on 2018/11/21/0021 19:32 .
 */
@Service
public class DataGroupService {

  // 获取数据源分组信息
  // 添加数据源分组
  // 修改数据源分组
  // 删除数据源分组

  @Autowired
  private DataGroupDao dataGroupDao;


  /**
   * 获取数据源分组信息
   * @param userId
   * @param offset
   * @param limit
   * @return
   */
  public List<DataGroup> getDataGroup(Integer userId, int offset, int limit) {

    return dataGroupDao.getDataGroup(userId, offset, limit);
  }

  /**
   * 添加数据源分组
   * @param dataGroup
   * @return
   */
  public Result addDataGroup(DataGroup dataGroup) {

    // 过滤HTML
    dataGroup.setGroupName(HtmlUtils.htmlEscape(dataGroup.getGroupName()));

    dataGroupDao.addDataGroup(dataGroup);

    if(dataGroup.getId() == null)
      return ResultUtil.error(ResultCode.FAIL.getCode(), "添加失败");

    return ResultUtil.success();
  }

  /**
   * 修改数据源分组(仅能修改名称)
   * @param dataGroup
   * @return
   */
  public Result updateDataGroup(DataGroup dataGroup) {

    if(dataGroup.getId() == null) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "缺失数据源分组id");
    } else if(StringUtils.isEmpty(dataGroup.getGroupName())) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "缺失数据集名称");
    } else {
      // 过滤HTML
      dataGroup.setGroupName(HtmlUtils.htmlEscape(dataGroup.getGroupName()));
      dataGroupDao.updateDataGroup(dataGroup);
    }

    return ResultUtil.success();
  }

  /**
   * 删除数据源分组
   * @param id
   * @return
   */
  public Result deleteDataGroup(Integer id) {

    dataGroupDao.deleteDataGroup(id);

    return ResultUtil.success();
  }


}
