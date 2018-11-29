package cn.goldlone.bigdata_platform;

import cn.goldlone.bigdata_platform.utils.HDFSUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author Created by CN on 2018/11/21/0021 23:30 .
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class DataSourceTest {


  @Autowired
  private HDFSUtil hdfsUtil;

  @Test
  public void mkdir() throws Exception {

    hdfsUtil.mkdirs("test/mm1/mm2");

  }


  public static void main(String[] args) {
    String folder = System.getProperty("java.io.tmpdir");
    System.out.println(folder);
  }

}
