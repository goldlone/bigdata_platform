package cn.goldlone.bigdata_platform;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BigdataPlatformApplicationTests {

  @Test
  public void contextLoads() {
  }


  @Value("${cluster.hostname}")
  private String hostname;

  @Value("${cluster.username}")
  private String username;

  @Test
  public void testYml() {
    System.out.println(hostname);
    System.out.println(username);
  }
}
