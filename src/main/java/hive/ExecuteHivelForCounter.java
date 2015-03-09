package hive;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExecuteHivelForCounter {
	
	public static String hiveSql="create table if not exist logInfo(rdate String,time Array<String>, type String,relateclass String ,information1 String,information2 String,information3 String) "
			+ "rowformat delimited fields terminated by '' collection items terminated by ',' map keys terminated ':'";

	//����Ĳ�������־���� ������
	public static void main(String[] args) throws SQLException {
		if(args.length<2){
			System.out.println("�������� ��־���� ������");
		}
		String type=args[0];
		String date=args[1];
		HiveUtil.createTable(hiveSql);
		HiveUtil.loadDate("load data local inpath '~/hadooplog/*' overwrite into table loginfo ");
		ResultSet hiResultSet = HiveUtil.queryHive("select rdate,time[0],type,relativeclass,information1,information2 where type='"+type+"'and rdate='"+date+",");
		HiveUtil.hiveToMysql(hiResultSet);
		HiveUtil.close();
		MysqlUtil.close();
	}
	//����ǰ��Ҫ��װ��hive �Ļ���������hiveServer ��ָ��һ���˿ڼ���
	//$ hive --server hiverserver 50031 
}
