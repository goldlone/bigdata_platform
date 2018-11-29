package cn.goldlone.bigdata_platform;

import cn.goldlone.bigdata_platform.service.DataFlowService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Created by CN on 2018/11/26/0026 10:28 .
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class DataFlowTest {

  @Autowired
  private DataFlowService dataFlowService;

  @Test
  public void updateStatus() {
    Integer dataFlowId = 1;
    String status = "Success";

    dataFlowService.updateDataFlowStatus(dataFlowId, status);
  }


}
