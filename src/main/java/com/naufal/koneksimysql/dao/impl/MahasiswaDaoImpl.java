package com.naufal.koneksimysql.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.naufal.koneksimysql.dao.MahasiswaDao;
import com.naufal.koneksimysql.model.Mahasiswa;

public class MahasiswaDaoImpl implements MahasiswaDao {
	
	private final Connection connection;
	
	private final String INSERT = "INSERT INTO mahasiswa (nim, nama, ipk, jurusan) "
            + "	VALUES (?,?,?,?)";
    private final String UPDATE = "UPDATE mahasiswa SET nama=?, ipk=?, jurusan=? WHERE nim=?";
    private final String DELETE = "DELETE FROM mahasiswa WHERE nim=?";
    private final String SELECT_ALL = "SELECT nim,nama,ipk,jurusan FROM mahasiswa";
    private final String SELECT_BY_NIM = "SELECT nim,nama,ipk,jurusan FROM mahasiswa WHERE nim=?";

    public MahasiswaDaoImpl(Connection connection) {
        this.connection = connection;
    }

	@Override
	public boolean insert(Mahasiswa m) {
		
		PreparedStatement prepareStatement = null;
		
		try {
			prepareStatement = connection.prepareStatement(INSERT);
			prepareStatement.setString(1, m.getNim());
			prepareStatement.setString(2, m.getNama());
            prepareStatement.setFloat(3, m.getIpk());
            prepareStatement.setString(4, m.getJurusan());
            return prepareStatement.executeUpdate() > 0;
		}catch (SQLException ex) {
            Logger.getLogger(MahasiswaDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (prepareStatement != null) {
                try {
                    prepareStatement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(MahasiswaDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
		return false;
	}

	@Override
	public boolean update(Mahasiswa m) {
		
		PreparedStatement prepareStatement = null;
		
		try {
			prepareStatement = connection.prepareStatement(UPDATE);
			prepareStatement.setString(1, m.getNama());
			prepareStatement.setFloat(2, m.getIpk());
			prepareStatement.setString(3, m.getJurusan());
            prepareStatement.setString(4, m.getNim());
            return prepareStatement.executeUpdate() > 0;
		}catch (SQLException ex) {
            Logger.getLogger(MahasiswaDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (prepareStatement != null) {
                try {
                    prepareStatement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(MahasiswaDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
		return false;
	}

	@Override
	public boolean delete(String nim) {
		
		PreparedStatement prepareStatement = null;
        try {
            prepareStatement = connection.prepareStatement(DELETE);
            prepareStatement.setString(1, nim);
            return prepareStatement.executeUpdate() > 0;
        } catch (SQLException ex) {
            Logger.getLogger(MahasiswaDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            if (prepareStatement != null) {
                try {
                    prepareStatement.close();
                } catch (SQLException ex) {
                    Logger.getLogger(MahasiswaDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }
        return false;
		
	}

	@Override
	public Mahasiswa getMahasiswaByNim(String nim) {
		
		PreparedStatement prepareStatement = null;
		ResultSet executeQuery = null;
		Mahasiswa m = null;
		
		try {
			prepareStatement = connection.prepareStatement(SELECT_BY_NIM);
			prepareStatement.setString(1, nim);
			executeQuery = prepareStatement.executeQuery();
			if(executeQuery.next()) {
				System.out.println(""+SELECT_BY_NIM);
                m = new Mahasiswa(executeQuery.getNString("nim"), 
                		executeQuery.getString("nama"), 
                		executeQuery.getFloat("ipk"), 
                		executeQuery.getString("jurusan"));
			}
		}catch (SQLException ex) {
            Logger.getLogger(MahasiswaDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
        	
            try {
                if (prepareStatement != null) {
                    prepareStatement.close();
                }
                if (executeQuery != null) {
                    executeQuery.close();
                }
            } catch (SQLException ex) {
                Logger.getLogger(MahasiswaDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
        return m;
		
	}

	@Override
	public List<Mahasiswa> getAllMahasiswa() {
		
		List<Mahasiswa> mahasiswas = new ArrayList<>();
        Statement statement = null;
        ResultSet executeQuery = null;
        try {
            statement = connection.createStatement();
            executeQuery = statement.executeQuery(SELECT_ALL);
            while (executeQuery.next()) {
                Mahasiswa m = new Mahasiswa(executeQuery.getNString("nim"), 
                		executeQuery.getString("nama"), 
                		executeQuery.getFloat("ipk"), 
                		executeQuery.getString("jurusan"));
                mahasiswas.add(m);
            }
        } catch (SQLException ex) {
            Logger.getLogger(MahasiswaDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {

            try {
                if (statement != null) {
                    statement.close();
                }
                if (executeQuery != null) {
                    executeQuery.close();
                }
            } catch (SQLException ex) {
                Logger.getLogger(MahasiswaDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
        return mahasiswas;
		
	}

}
