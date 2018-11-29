package cn.goldlone.bigdata_platform;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

@Component
public class RemoteUtilTest {

	private static String DEFAULTCHART = "UTF-8";
	private Connection conn;

	private String host;

	private String username;

	private String password;

  public RemoteUtilTest() {}

	public RemoteUtilTest(String host, String username, String password) {
		this.host = host;
		this.username = username;
		this.password = password;
	}

	public Boolean login() {
		boolean flg = false;
		try {
			conn = new Connection(host);
			conn.connect();// 连接
			flg = conn.authenticateWithPassword(username, password);// 认证
		} catch (IOException e) {
			e.printStackTrace();
		}
		return flg;
	}

	public String execute(String cmd) {
		String result = "";
		try {
			if (login()) {
				System.out.println("登录成功");
				Session session = conn.openSession();// 打开一个会话
				session.execCommand(cmd);// 执行命令
				result = processStdout(session.getStdout(), DEFAULTCHART);
				// 如果为得到标准输出为空，说明脚本执行出错了
				if (StringUtils.isEmpty(result)) {
					result = processStdout(session.getStderr(), DEFAULTCHART);
				}
				conn.close();
				session.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	private String processStdout(InputStream in, String charset) {
		InputStream stdout = new StreamGobbler(in);
		StringBuffer buffer = new StringBuffer();
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(stdout, charset));
			String line = null;
			while ((line = br.readLine()) != null) {
				buffer.append(line + "\n");
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return buffer.toString();
	}

	public static void setCharset(String charset) {
		DEFAULTCHART = charset;
	}

	public static void main(String[] args) {
		RemoteUtilTest remoteUtil = new RemoteUtilTest("hh1","hduser","hduser");
		String result = remoteUtil.execute("source /etc/profile && hive -S -e " +
            "\"create table bigdata_platform.test(id int, name string) row format delimited fields terminated by '\\t';\"");
		System.out.println(result);
	}

}
