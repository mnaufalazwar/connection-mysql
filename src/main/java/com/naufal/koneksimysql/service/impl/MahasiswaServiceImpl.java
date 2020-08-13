package com.naufal.koneksimysql.service.impl;

import java.util.List;

import com.naufal.koneksimysql.dao.MahasiswaDao;
import com.naufal.koneksimysql.model.Mahasiswa;
import com.naufal.koneksimysql.service.MahasiswaService;

public class MahasiswaServiceImpl implements MahasiswaService {

	private final MahasiswaDao mahasiswaDao;
	
	public MahasiswaServiceImpl(MahasiswaDao mahasiswaDao) {
        this.mahasiswaDao = mahasiswaDao;
    }
	
	@Override
	public boolean insert(Mahasiswa m) {
		// TODO Auto-generated method stub
		return mahasiswaDao.insert(m);
	}

	@Override
	public boolean update(Mahasiswa m) {
		// TODO Auto-generated method stub
		return mahasiswaDao.update(m);
	}

	@Override
	public boolean delete(String nim) {
		// TODO Auto-generated method stub
		return mahasiswaDao.delete(nim);
	}

	@Override
	public Mahasiswa getMahasiswaByNim(String nim) {
		// TODO Auto-generated method stub
		return mahasiswaDao.getMahasiswaByNim(nim);
	}

	@Override
	public List<Mahasiswa> getAllMahasiswa() {
		// TODO Auto-generated method stub
		return mahasiswaDao.getAllMahasiswa();
	}

}
