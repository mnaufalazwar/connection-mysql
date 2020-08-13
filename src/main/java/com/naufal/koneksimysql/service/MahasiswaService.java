package com.naufal.koneksimysql.service;

import java.util.List;

import com.naufal.koneksimysql.model.Mahasiswa;

public interface MahasiswaService {

	public boolean insert(Mahasiswa m);

    public boolean update(Mahasiswa m);

    public boolean delete(String nim);

    public Mahasiswa getMahasiswaByNim(String nim);

    public List<Mahasiswa> getAllMahasiswa();
    
}
