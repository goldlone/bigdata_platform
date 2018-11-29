package cn.goldlone.bigdata_platform.service;


import cn.goldlone.bigdata_platform.async.EventModel;
import cn.goldlone.bigdata_platform.async.EventProducer;
import cn.goldlone.bigdata_platform.async.EventType;
import cn.goldlone.bigdata_platform.dao.DataSourceDao;
import cn.goldlone.bigdata_platform.model.*;
import cn.goldlone.bigdata_platform.utils.HDFSUtil;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Created by CN on 2018/11/21/0021 19:28 .
 */
@Service
public class DataSourceService {

  // 查询数据源信息（分页）
  // 添加数据源
  // 修改数据源信息（注意移动）
  // 删除数据源


  // 预览数据源（前n行）

  // 本地文件上传至HDFS
  // 本地文件增量上传至HDFS
  // 数据库拉取至HDFS
  // 数据库增量拉取至HDFS

  // 本地文件上传至Hive
  // 本地文件增量上传Hive
  // 数据库拉取至Hive
  // 数据库增量拉取至Hive

  @Autowired
  private DataSourceDao dataSourceDao;

  @Autowired
  private HDFSUtil hdfsUtil;

  @Autowired
  private EventProducer eventProducer;

  @Autowired
  private HostHolder hostHolder;

  /**
   * 预览前n行数据
   */
  public Result previewTopNData(Integer sourceId, int n) {
    DataSource dataSource = dataSourceDao.getDataSourceById(sourceId);

    if(dataSource == null) {
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "数据源不存在");
    }

    List<String> list = null;
    String path = "";
    if(dataSource.getSourceType().equals("hdfs")) {

      // 从hdfs中读取前n几行
      path = dataSource.getHdfsPath();
    } else {

      // 从hive中读取前n行 => /user/hive/warehouse/bigdata_platform.db/{表名}
      path = "/user/hive/warehouse/bigdata_platform.db/" + dataSource.getHiveTable();
    }

    if(n > 1) {
      list = hdfsUtil.readNLine(path, n);
    }

    return ResultUtil.success("查询成功", list);
  }


  /**
   * 查询数据源信息（分页）
   * @param userId
   * @param offset
   * @param limit
   * @return
   */
  public List<DataSource> getDataSource(int userId, int offset, int limit) {

    return dataSourceDao.getDataSource(userId, offset, limit);
  }


  public List<DataSource> selectAllByType(Integer userId, String type) {

    return dataSourceDao.selectAllByType(userId, type);
  }


  /**
   * 数据源总数
   * @return
   */
  public int getDataSourceCount(int userId) {

    return dataSourceDao.getDataSourceCount(userId);
  }

  /**
   * 根据id获取数据源
   * @param sourceId
   * @return
   */
  public DataSource getDataSourceById(int sourceId) {

    return dataSourceDao.getDataSourceById(sourceId);
  }


  /**
   * 添加数据源
   * @param dataSource
   * @return
   */
  public Map<String, String> addDataSource(DataSource dataSource) {
    Map<String, String> map = new HashMap<>();

    dataSourceDao.addDataSource(dataSource);

    if(dataSource.getId() == null) {
      map.put("msg", "添加失败");
    }
    return map;
  }

  /**
   * 修改数据源信息，仅能修改名称、分组、修改时间
   * @param dataSource
   * @return
   */
  public Map<String, String> updateDataSource(DataSource dataSource) {
    Map<String, String> map = new HashMap<>();

    if(dataSource.getId() == null) {
      map.put("msg", "缺失数据集分组id");
    } else {
      dataSourceDao.updateDataSource(dataSource);
    }

    return map;
  }

  /**
   * 删除数据源
   * @param id
   * @return
   */
  public Result deleteDataSource(int id) {

    // 1. 获取数据源信息
    DataSource dataSource = dataSourceDao.getDataSourceById(id);
    if(dataSource == null) {
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "该数据源不存在");
    }

    // 2. 检测数据源是否为当前用户所有
    if(!hostHolder.getUser().getId().equals(dataSource.getUserId())) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "非法的越权访问");
    }

    // 3. 发送删除数据源事件
    eventProducer.emitEvent(new EventModel(EventType.DELETE_DATASOURCE)
            .setActorId(dataSource.getUserId())
            .setExts("dataSource", JSONObject.toJSONString(dataSource)));

    return ResultUtil.success();
  }
}
