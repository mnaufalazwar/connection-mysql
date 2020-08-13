package com.naufal.koneksimysql.util;

import com.naufal.koneksimysql.connection.MysqlConnection;
import com.naufal.koneksimysql.dao.MahasiswaDao;
import com.naufal.koneksimysql.dao.impl.MahasiswaDaoImpl;

public class JdbcUtils {

	 private static MahasiswaDao mahasiswaDao;
	 
	 public static MahasiswaDao getMahasiswaDao() {
		 if (mahasiswaDao == null) {
			 mahasiswaDao = new MahasiswaDaoImpl(MysqlConnection.getConnection());
		 }
		 return mahasiswaDao;
	 }
	 
}
