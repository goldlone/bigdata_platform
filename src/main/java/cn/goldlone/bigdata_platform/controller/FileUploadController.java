package cn.goldlone.bigdata_platform.controller;

import cn.goldlone.bigdata_platform.model.DataSource;
import cn.goldlone.bigdata_platform.model.HostHolder;
import cn.goldlone.bigdata_platform.model.Result;
import cn.goldlone.bigdata_platform.model.ResultCode;
import cn.goldlone.bigdata_platform.service.FileUploadService;
import cn.goldlone.bigdata_platform.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 本地文件上传
 * @author Created by CN on 2018/11/22/0022 14:24 .
 */
@RestController
@RequestMapping("/file/upload")
public class FileUploadController {


  @Autowired
  private FileUploadService fileUploadService;

  @Autowired
  private HostHolder hostHolder;

  /**
   * 本地文件上传至HDFS
   * @param sourceName
   * @param sourceType
   * @param file
   * @param groupId
   * @return
   */
  @PostMapping("/hdfs")
  public Result fileUploadHDFS(@RequestParam("sourceName") String sourceName,
                               @RequestParam("sourceType") String sourceType,
                               @RequestParam("file") MultipartFile file,
                               @RequestParam("groupId") Integer groupId) {

    DataSource dataSource = new DataSource();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dataSource.setCreateDate(sdf.format(new Date()));
    dataSource.setModifyData(sdf.format(new Date()));
    dataSource.setSourceName(sourceName);
    dataSource.setSourceType(sourceType);
    dataSource.setGroupId(groupId);
    dataSource.setUserId(hostHolder.getUser().getId());

    return fileUploadService.fileUploadHDFS(file, dataSource);
  }


  /**
   * 本地文件增量导入HDFS
   * @param sourceId 数据源id
   * @param file 文件
   * @param approach “append” | "overwrite"
   * @return
   */
  @PostMapping("/hdfsPlus")
  public Result fileUploadHDFSPlus(@RequestParam("sourceId") Integer sourceId,
                                   @RequestParam("file") MultipartFile file,
                                   @RequestParam("approach") String approach) {

    if(approach==null || (!approach.equals("append") &&
            !approach.equals("overwrite"))) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "非法的导入方式");
    }

    return fileUploadService.fileUploadHDFSPlus(sourceId, file, approach);
  }


//  public Result fileUploadHive(@RequestParam("sourceName") String sourceName,
//                               @RequestParam("sourceType") String sourceType,
//                               @RequestParam("file") MultipartFile file,
//                               @RequestParam("groupId") Integer groupId,
//                               @RequestParam("") String hiveTable,
//                               @RequestParam("") List<String> hiveColum) {
//
//  }


  /**
   * 本地文件导入Hive
   * @param sourceName
   * @param sourceType
   * @param groupId
   * @param fieldTerminated
   * @param columns
   * @param dataTypes
   * @param file
   * @return
   */
  @PostMapping("/hive")
  public Result fileUploadHive(@RequestParam(value = "sourceName") String sourceName,
                     @RequestParam(value = "sourceType") String sourceType,
                     @RequestParam(value = "groupId") Integer groupId,
                     @RequestParam(value = "fieldTerminated") String fieldTerminated,
                     @RequestParam(value = "columns") String[] columns,
                     @RequestParam(value = "dataTypes") String[] dataTypes,
                     @RequestParam("file") MultipartFile file) {

//    System.out.println(sourceName);
//    System.out.println(sourceType);
//    System.out.println(groupId);
//
//    System.out.println(columns.length);
//    System.out.println(dataTypes.length);

//    JSONObject columns = JSONObject.parseObject(columnsStr);
//    for(String key : columns.keySet()) {
//      System.out.println(key + " => " + columns.get(key));
//    }
//
    DataSource dataSource = new DataSource();
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    dataSource.setCreateDate(sdf.format(new Date()));
    dataSource.setModifyData(sdf.format(new Date()));
    dataSource.setSourceName(sourceName);
    dataSource.setSourceType(sourceType);
    dataSource.setGroupId(groupId);
    dataSource.setUserId(hostHolder.getUser().getId());

    return fileUploadService.fileUploadHive(dataSource, file, fieldTerminated, columns, dataTypes);
  }


  @PostMapping("/hivePlus")
  public Result fileUploadHivePlus(@RequestParam("sourceId") Integer sourceId,
                                   @RequestParam("approach") String approach,
                                   @RequestParam("file") MultipartFile file) {

    if(approach==null || (!approach.equals("append") &&
            !approach.equals("overwrite"))) {
      return ResultUtil.error(ResultCode.FAIL.getCode(), "非法的导入方式");
    }

    return fileUploadService.fileUploadHivePlus(sourceId, approach, file);
  }


}
