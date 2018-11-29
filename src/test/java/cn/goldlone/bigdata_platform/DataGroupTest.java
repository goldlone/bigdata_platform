package cn.goldlone.bigdata_platform;

import cn.goldlone.bigdata_platform.model.DataGroup;
import cn.goldlone.bigdata_platform.service.DataGroupService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

/**
 * @author Created by CN on 2018/11/21/0021 21:00 .
 */

@SpringBootTest
@RunWith(SpringRunner.class)
public class DataGroupTest {

  @Autowired
  private DataGroupService dataGroupService;


  @Test
  public void add() {

    DataGroup dg1 = new DataGroup("医疗", 1);
    DataGroup dg2 = new DataGroup("交通", 1);
    DataGroup dg3 = new DataGroup("交通", 2);

    dataGroupService.addDataGroup(dg1);
    dataGroupService.addDataGroup(dg2);
    dataGroupService.addDataGroup(dg3);
    dataGroupService.addDataGroup(new DataGroup("金融", 1));
    dataGroupService.addDataGroup(new DataGroup("日志", 1));
    dataGroupService.addDataGroup(new DataGroup("电商", 1));
  }

  @Test
  public void select() {
    int[] userIds = {1, 2};
    List<DataGroup> list = null;

    for(int id : userIds) {
      list = dataGroupService.getDataGroup(id, 0, Integer.MAX_VALUE);
      if(list == null)
        continue;
      for(DataGroup dg : list) {
        System.out.println("【"+id+"】 => " + dg.toString());
      }
    }
  }


  @Test
  public void updateName() {

    DataGroup dg = new DataGroup(3, "金融");
    dataGroupService.updateDataGroup(dg);
  }

  @Test
  public void delete() {
    dataGroupService.deleteDataGroup(1);
  }

}
