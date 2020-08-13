package com.naufal.koneksimysql.controller;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.naufal.koneksimysql.model.Mahasiswa;
import com.naufal.koneksimysql.service.MahasiswaService;
import com.naufal.koneksimysql.service.impl.MahasiswaServiceImpl;
import com.naufal.koneksimysql.util.JdbcUtils;

@RestController
@RequestMapping(value="/con")
public class Controller {
	
	private MahasiswaService mahasiswaService = new MahasiswaServiceImpl(JdbcUtils.getMahasiswaDao());

	@RequestMapping(value="/insert", method=RequestMethod.PUT)
	public String insertMahasiswa(@RequestBody Mahasiswa mahasiswa) {
		
		if(mahasiswaService.insert(mahasiswa)) {
			return "insert success";
		}
		else {
			return "insert failsed";
		}
		
	}
	
}
