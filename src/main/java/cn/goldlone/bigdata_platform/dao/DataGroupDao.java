package cn.goldlone.bigdata_platform.dao;

import cn.goldlone.bigdata_platform.model.DataGroup;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author Created by CN on 2018/11/21/0021 19:52 .
 */
@Mapper
public interface DataGroupDao {


  List<DataGroup> getDataGroup(@Param("userId") Integer userId,
                               @Param("offset") int offset,
                               @Param("limit") int limit);


  void addDataGroup(DataGroup dataGroup);


  void updateDataGroup(DataGroup dataGroup);

  void deleteDataGroup(Integer id);
}
