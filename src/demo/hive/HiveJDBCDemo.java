package demo.hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;


import demo.utils.JDBCUtils;

public class HiveJDBCDemo {
	public static void main(String[] args){
		Connection conn = null;
		Statement st = null;
		ResultSet rs = null;
		
		String tableName = "emp";
		String sql = null;
		try{
			//获取连接
			conn = JDBCUtils.getConnection();
			//创建运行环境
			st = conn.createStatement();
			//运行HQL
			
			//创建表
			/*
			st.execute("drop table if exists " + tableName);
			st.execute("create table " + tableName + "(name string,sal double) row format delimited fields terminated by '\t'");
			System.out.println("create table success!");
			*/
			
			
			//查询表
			/*
			rs = st.executeQuery("desc " + tableName);
			while(rs.next()){
				System.out.println(rs.getString(1)+"\t"+rs.getString(2));
			}
			System.out.println("desc table success!");
			*/
			
			//导入数据
			/*
			String filePath = "/tmp/emp.txt";  //为hive运行的服务器目录
			sql = "load data local inpath '" + filePath + "' into table " + tableName;
			st.execute(sql);
			System.out.println("load data into table success!");
			*/
			
			//运行HQL
			sql = "select * from emp";
			rs = st.executeQuery(sql);
			//处理数据
			while(rs.next()){
				//姓名和薪水
				String name = rs.getString(1);
				double sal = rs.getDouble(2);
				System.out.print(name+":"+sal+"\n");
			}
		}catch (Exception e) {
			e.printStackTrace();
		}finally{
			JDBCUtils.release(conn, st, rs);
		}
	}
}
