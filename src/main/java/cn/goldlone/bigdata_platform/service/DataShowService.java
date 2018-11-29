package cn.goldlone.bigdata_platform.service;

import cn.goldlone.bigdata_platform.dao.DataFlowDao;
import cn.goldlone.bigdata_platform.dao.DataSourceDao;
import cn.goldlone.bigdata_platform.model.DataFlow;
import cn.goldlone.bigdata_platform.model.DataSource;
import cn.goldlone.bigdata_platform.model.Result;
import cn.goldlone.bigdata_platform.model.ResultCode;
import cn.goldlone.bigdata_platform.utils.HiveUtil;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Created by CN on 2018/11/28/0028 17:43 .
 */
@Service
public class DataShowService {


  @Autowired
  private HiveUtil hiveUtil;

  @Value("${cluster.hive.database}")
  private String hiveDatabase;

  @Autowired
  private DataSourceDao dataSourceDao;

  @Autowired
  private DataFlowDao dataFlowDao;


  public Result getDataSourceDataFromHive(int dataSourceId) {
    DataSource dataSource = dataSourceDao.getDataSourceById(dataSourceId);

    if(dataSource == null)
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "数据源不存在");

    if(!dataSource.getSourceType().equals("hive"))
      return ResultUtil.error(ResultCode.FAIL.getCode(), "不支持非Hive类型数据");

    List<String> columns = this.getHiveTableInfo(dataSource.getHiveTable());
    List<String> list = this.getHiveTableData(dataSource.getHiveTable());

    Map<String, List<String>> map = new HashMap<>();
    for(int i=0; i<columns.size(); i++) {
      String colName = columns.get(i).split("\t")[0];
      List<String> data = new ArrayList<>();

      for(String str : list) {
        String[] fields = str.split("\t");
        if(i < fields.length)
          data.add(fields[i]);
        else
          data.add("null");
      }

      map.put(colName, data);
    }

    return ResultUtil.success("查询成功", map);
  }


  public Result getDataFlowResultDataFromHive(int dataFlowId) {
    DataFlow dataFlow = dataFlowDao.getDataFlowById(dataFlowId);

    if(dataFlow == null)
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "数据源不存在");

    if(!dataFlow.getFlowType().equals("hql") || !dataFlow.getFlowStatus().equals("Success"))
      return ResultUtil.error(ResultCode.FAIL.getCode(), "当前状态不支持获取数据");

    List<String> columns = this.getHiveTableInfo(dataFlow.getResultTable());
    List<String> list = this.getHiveTableData(dataFlow.getResultTable());

    Map<String, List<String>> map = new HashMap<>();
    for(int i=0; i<columns.size(); i++) {
      String colName = columns.get(i).split("\t")[0];
      List<String> data = new ArrayList<>();

      for(String str : list) {
        String[] fields = str.split("\t");
        if(i < fields.length)
          data.add(fields[i]);
        else
          data.add("null");
      }

      map.put(colName, data);
    }

    return ResultUtil.success("查询成功", map);
  }


  public Result getDataFlowInfoFromHive(int dataFlowId) {

    DataFlow dataFlow = dataFlowDao.getDataFlowById(dataFlowId);

    if(dataFlow == null)
      return ResultUtil.error(ResultCode.ENTITY_NOT_EXISTS.getCode(), "数据源不存在");

    if(!dataFlow.getFlowType().equals("hql") || !dataFlow.getFlowStatus().equals("Success"))
      return ResultUtil.error(ResultCode.FAIL.getCode(), "当前状态不支持获取数据");


    List<String> list = this.getHiveTableInfo(dataFlow.getResultTable());
    List<String> res = new ArrayList<>();
    list.forEach(str -> {
      res.add(str.split("\t")[0]);
    });

    return ResultUtil.success("查询成功", res);
  }


  private List<String> getHiveTableInfo(String tableName) {

    return hiveUtil.getTableInfo(hiveDatabase + "." + tableName);
  }


  private List<String> getHiveTableData(String tableName) {

    return hiveUtil.getTableData(hiveDatabase + "." + tableName);
  }

}
