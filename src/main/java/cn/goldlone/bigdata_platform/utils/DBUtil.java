package cn.goldlone.bigdata_platform.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DBUtil {

    // 0.定义工具类相关组件
    private Connection connection = null;
    private PreparedStatement preparedStatement = null;
    private ResultSet resultSet = null;
    private Integer intResult = -1;
//    private static PropertiesUtil propertiesUtil = new PropertiesUtil("config/db.properties");

    /**
     * 在使用该类一定会执行，并且只需要执行一次的步骤，如：加载驱动
     */
    static {
        // 1.加载驱动
        try {
//            String driverClass = propertiesUtil.readPropertyByKey("driverClass");
            String driverClass = "com.mysql.jdbc.Driver";
            Class.forName(driverClass);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 在构造方法中执行必须的步骤，如:打开连接
     */
    public DBUtil(String url, String username, String password) {
        // 2.打开连接
//        open();
      try {
        connection = DriverManager.getConnection(url, username, password);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }

    /**
     * 通过配置文件的方式打开连接
     */
    public void open() {
//        try {
//            // 从配置文件中读取数据库连接参数
//            String url = propertiesUtil.readPropertyByKey("url");
//            String userName = propertiesUtil.readPropertyByKey("userName");
//            String password = propertiesUtil.readPropertyByKey("password");
//            // 2.打开数据库连接
//
//        } catch (SQLException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
    }

    /**
     * 执行更新操作
     * @param sql 需要执行的sql
     * @return 数据库中变更的条数，通常用于判断该操作是否成功执行
     */
    public int executeUpdate(String sql,Object[] objects) {
        try {
            // 3.通过传入的参数初始化preparedStatement
            preparedStatement = connection.prepareStatement(sql);
            // 当传入的数组非空时则需要进行赋值操作
            if (objects != null) {
                setPreparedStatement(objects);
            }
            // 4.5.操作数据库并接收返回结果
            intResult = preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            intResult = -1;
        } finally {
            return intResult;
        }
    }

    //**直接执行指定的sql命令
    public void execute(String sql) {
        try {
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.execute();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * 执行查询操作
     * @param sql 需要执行的sql
     * @return ResultSet对象
     */
    public ResultSet executeQuery(String sql,Object[] objects) {
        try {
            // 3.通过传入的参数初始化preparedStatement
            preparedStatement = connection.prepareStatement(sql);
            // 当传入的数组非空时则需要进行赋值操作
            if (objects != null) {
                setPreparedStatement(objects);
            }
            // 4.5.操作数据库并接收返回结果
            resultSet = preparedStatement.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            resultSet = null;
        } finally {
            return resultSet;
        }
    }

    /**
     * 对于已经初始化完毕的preparedStatement进行参数赋值
     * @param objects
     */
    public void setPreparedStatement(Object[] objects) {
        // for循环遍历参数数组
        for (int i = 0; i < objects.length; i++) {
            try {
                // 参数设置的下标与数组下标相差1
                preparedStatement.setObject(i + 1, objects[i]);
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public List<String> getTableData(String tableName){
        List<String> list = new ArrayList<>();
        try {
            // 3.通过传入的参数初始化preparedStatement
            int columnSize = getTableInfo(tableName).size();
            String sql = "select * from " + tableName;
            preparedStatement = connection.prepareStatement(sql);
            // 4.5.操作数据库并接收返回结果
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                String temp = "";
                for (int i = 1;i <=columnSize;i ++){
                    temp += resultSet.getString(i) + ",";
                }
                temp = temp.substring(0, temp.length() - 1);
                list.add(temp);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            list = null;
        } finally {
            return list;
        }
    }

    public List<String> getTableInfo(String tableName){
        List<String> list = new ArrayList<>();
        try {
            // 3.通过传入的参数初始化preparedStatement
            String sql = "desc " + tableName;
            preparedStatement = connection.prepareStatement(sql);
            // 4.5.操作数据库并接收返回结果
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                String temp = resultSet.getString(1) + "," + resultSet.getString(2);
                list.add(temp);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            list = null;
        } finally {
            return list;
        }
    }

    // 6.释放资源
    public void close() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

}
