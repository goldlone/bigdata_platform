package cn.goldlone.bigdata_platform.utils;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component
public class HiveUtil implements InitializingBean {
	
	private Statement statement = null;

	@Value("${cluster.hive.url}")
	private String url;

	@Value("${cluster.hive.username}")
	private String username;

	@Value("${cluster.hive.password}")
	private String password;

	public HiveUtil() {
//		open();
//		init();
	}
	
	static {
		try {
			// 1.加载驱动
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}


  @Override
  public void afterPropertiesSet() throws Exception {
	  open();
  }

//  public static void main(String[] args) {
//	  HiveUtil hiveUtil = new HiveUtil();
//	  hiveUtil.open();
//
//	  List<String> list = hiveUtil.getTableData("bigdata_platform.1_1_1543156486755");
//	  for(String item : list) {
//      System.out.println(item);
//    }
//  }

	private void open() {
		try {
			// 2.打开连接
			Connection connection = DriverManager.getConnection(url, username, password);
//			Connection connection = DriverManager.getConnection("jdbc:hive2://hh:10000/", "bigdata", "bigdata");
			// 3.获得操作对象 - 会话
			statement = connection.createStatement();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void init() {
//		try {
//			//statement.execute("create temporary function sub as 'com.sand.udf.SubString'");
//			//statement.execute("create temporary function jsonParse as 'com.sand.udf.JsonParse'");
//			//statement.execute("create temporary function split1 as 'com.sand.udtf.Split'");
//			//statement.execute("create temporary function concat1 as 'com.sand.udaf.Concat'");
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
	}
	
	/**
	 * 创建数据库 - 用户注册时调用
	 * @param databaseName 根据用户标识生成的数据库名称
	 */
	public void createDatabase(String databaseName) {
		try {
			statement.execute("create database " + databaseName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	//TODO:获得数据库
	public List<String> showDatabases(){
		ResultSet rs;
		List<String> list = new ArrayList<>();
		try {
			rs = statement.executeQuery("show databases");			
			while(rs.next()) {
				list.add(rs.getString(1));
			}	
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();		
		}
		return list;
	}
	
	/**
	 * 切换数据库 - 只对当前会话有效
	 * @param databaseName 目标数据库名称
	 */
	public void changeDatabase(String databaseName) {
		try {
			statement.execute("use " + databaseName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 获得当前数据库中的数据列表 - 注意切换数据库
	 * @return 数据表名称的集合
	 */
	public List<String> getTaleList() {
		List<String> list = new ArrayList<>();
		try {
			ResultSet rs = statement.executeQuery("show tables");
			while (rs.next()) {
				list.add(rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	//TODO:执行HQL语句
	public void execute(String HQL) {
		try {
			statement.execute(HQL);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//TODO:在指定数据库中创建表，必须确保指定的数据库存在
	public void createTable(String toDatabase,String tableName,String[]col_nameList,String[]col_typeList,String format,int skipCount) {
		changeDatabase(toDatabase);
		String HQL = "create table " + tableName + "(";
		for(int i = 0;i < col_nameList.length;i++) {
			HQL += col_nameList[i] + " " + col_typeList[i] + ",";
		}
		HQL = HQL.substring(0, HQL.length()-1);
		HQL += ")row format delimited fields terminated by '" + format  + "' tblproperties(\"skip.header.line.count\"=\"" + skipCount +"\")";
		System.out.println(HQL);
		try {
			statement.execute(HQL);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	/**
	 * 获得数据表的简要信息
	 * @param tableName 数据表名称
	 * @return 列名及列的数据类型
	 */
	public List<String> getTableInfo(String tableName){
		List<String> list = new ArrayList<>();
		try {
			ResultSet rs = statement.executeQuery("desc " + tableName);
			while (rs.next()) {
				list.add(rs.getString(1) + "\t" + rs.getString(2));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	
	/**
	 * 获取数据表前十条的预览数据
	 * @param tableName 数据表名称
	 * @return 数据表预览数据
	 */
	public List<String> getTableData(String tableName){
		List<String> list = new ArrayList<>();
		try {
			int size = getTableInfo(tableName).size();
			ResultSet rs = statement.executeQuery("select * from " + tableName);
			while (rs.next()) {
				String line = "";
				for(int i = 1;i <= size;i ++) {
					line += rs.getString(i) + "\t";
				}
				list.add(line);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}
	
	/**
	 * 获得查询sql执行后的返回结果
	 * @param sql 用户自定义sql
	 * @return sql执行结果集中的所有数据
	 */
	public List<String> getResultData(String sql){
		List<String> list = new ArrayList<>();
		// String databaseName = "data_flow";
		// 生成一个对于当前流程唯一的中间表名称
		// 如果流程会反复执行则先删除该表再创建
		String tableName = "data_flow_" + new Date().getTime();
		sql = "create table " + tableName + " as " + sql;
		try {
			// 执行查询语句，同时使用一个表进行记录
			statement.execute(sql);
			// 获得中间表的列信息 - 取决于用户执行sql的结果集结构
			int size = getTableInfo(tableName).size();
			ResultSet rs = statement.executeQuery("select * from " + tableName);
			while (rs.next()) {
				String line = "";
				for(int i = 1;i <= size;i ++) {
					line += rs.getString(i) + "\t";
				}
				list.add(line);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list;
	}

}
