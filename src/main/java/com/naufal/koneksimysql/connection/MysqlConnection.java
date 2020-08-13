package com.naufal.koneksimysql.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.mysql.cj.jdbc.MysqlDataSource;

public class MysqlConnection {
	
	private static Connection connection;
	
	public static Connection getConnection() {
		if(connection == null) {
			try {
				MysqlDataSource dataSource = new MysqlDataSource();
				dataSource.setUser("root");
				dataSource.setPassword("5November!");
				dataSource.setUrl("jdbc:mysql://localhost:3306/test-connection?useLegacyDatetimeCode=false&serverTimezone=UTC");
				connection = dataSource.getConnection();
			}
			catch(SQLException e) {
				Logger.getLogger(MysqlConnection.class.getName()).log(Level.SEVERE, null, e);
			}
		}
		return connection;
	}

}
