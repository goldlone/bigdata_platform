package cn.goldlone.bigdata_platform;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


/**
 * @author Created by CN on 2018/11/22/0022 17:27 .
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class MySQLImportTest {

  private MockMvc mockMvc;

  @Autowired
  private WebApplicationContext wac;

  @Before
  public void setup() {
    this.mockMvc = MockMvcBuilders.webAppContextSetup(this.wac).build();
  }

  @Test
  public void testConnection() throws Exception {

    MvcResult result = mockMvc.perform(post("/mysql/import/testConnection")
            .contentType("application/x-www-form-urlencoded; charset=UTF-8")
            .param("address", "hh1")
            .param("port", "3306")
            .param("username", "hive")
            .param("password", "123456")) // 模拟发送post请求
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_UTF8)) // 预期返回值的媒体类型text/plain;charset=UTF-8
            .andReturn(); // 返回执行请求的结果

    System.out.println(result.getResponse().getContentAsString());
  }



  public static String mysqlImportHDFS(String address,
                                String port,
                                String database,
                                String username,
                                String password,
                                String table) {
    // completed successfully
    String url = "jdbc:mysql://" + address + ":" + port + "/" + database;
    String hdfsPath = "/bigdata_platform/user/gold/hdfs/1542876711111";
    String command = String.format("source /etc/profile && " +
            "sqoop import " +
            "--connect %s " +
            "--username %s " +
            "--password %s " +
            "--table %s " +
            "--target-dir %s", url, username, password, table, hdfsPath);

    return command;
  }

  public static void main(String[] args) {
    System.out.println(mysqlImportHDFS("hh1", "3306", "test", "hive", "123456", "make"));
  }
}
