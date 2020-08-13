package com.naufal.koneksimysql.dao;

import java.util.List;

import com.naufal.koneksimysql.model.Mahasiswa;

public interface MahasiswaDao {

	public boolean insert(Mahasiswa m);

    public boolean update(Mahasiswa m);

    public boolean delete(String nim);

    public Mahasiswa getMahasiswaByNim(String nim);

    public List<Mahasiswa> getAllMahasiswa();
    
}
