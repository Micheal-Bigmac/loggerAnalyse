package hive;

import java.sql.DriverManager;
import java.sql.SQLException;

import com.mysql.jdbc.Connection;

public class MysqlUtil {

	private static Connection connection=null;
	private static String url="jdbc:mysql://localhost:3306/hadoop?useUnicode=true&characterEncoding=UTF8";
	private static String password="mysql";
	private static String table="hive";
	static{
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {
			connection=(Connection) DriverManager.getConnection(url, table, password);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static Connection getMysqlConnection(){
		return connection;
	}
	public static void close() throws SQLException{
		connection.close();
	}
}
