package hive;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

public class HiveUtil {

	private static Connection connection=null;
	private static String url="jdbc:hive://localhost:50031/default";
	private static String table="hive";
	private static String password="mysql";
	static{
		try {
			Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
			connection=(Connection) DriverManager.getConnection(url,table,password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	public static Connection getHiveConnection(){
		return connection;
	}
	public static void close() throws SQLException{
		connection.close();
	}
	public static  void createTable(String hiveSql) throws SQLException{
		Statement statement=(Statement) connection.createStatement();
		ResultSet resultSet=statement.executeQuery(hiveSql);
	}
	public static ResultSet queryHive(String hiveSql) throws SQLException{
		Statement statement=(Statement) connection.createStatement();
		ResultSet resultSet=statement.executeQuery(hiveSql);
		return resultSet;
	}
	public static void loadDate(String hiveSql) throws SQLException{
		Statement statement=(Statement) connection.createStatement();
		ResultSet resultSet=statement.executeQuery(hiveSql);
	}
	public static void hiveToMysql(ResultSet hiResultSet) throws SQLException{
		Statement statement=(Statement) connection.createStatement();
		while(hiResultSet.next()){
			String rdate=hiResultSet.getString(1);
			String time=hiResultSet.getString(2);
			String type=hiResultSet.getString(3);
			String relateclass=hiResultSet.getString(4);
			String information=hiResultSet.getString(5)+hiResultSet.getString(6)+hiResultSet.getString(7);
			int i=statement.executeUpdate("insert into XXXLog values(0,'"+rdate+"','"+time+"','"+type+"','"+relateclass+"','"+information+"')");
		}
	}
}
