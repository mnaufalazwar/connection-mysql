CREATE TABLE IF NOT EXISTS mahasiswa(
nim VARCHAR(10) NOT NULL,
nama VARCHAR(50) NOT NULL,
ipk FLOAT(4,2) NOT NULL,
jurusan VARCHAR(25) NOT NULL,
PRIMARY KEY(nim)
)ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS alamat(
id_alamat INT AUTO_INCREMENT,
nim VARCHAR(10) NOT NULL, 
nama_jalan VARCHAR(50),
rt VARCHAR(3) NOT NULL,
rw VARCHAR(3) NOT NULL,
kode_desa VARCHAR(5) NOT NULL,
kode_kec VARCHAR(5) NOT NULL,
kode_kab VARCHAR(5) NOT NULL,
kode_prop VARCHAR(5) NOT NULL,
PRIMARY KEY(id_alamat)
)ENGINE=InnoDB;

ALTER TABLE alamat
ADD FOREIGN KEY (nim) REFERENCES mahasiswa(nim) ON DELETE CASCADE ON UPDATE CASCADE;

INSERT INTO mahasiswa VALUES('075410099','Noprianto',4.00,'Teknologi Informasi');
INSERT INTO mahasiswa VALUES('075410100','Noureen Akhlema Shannum',4.00,'Pendidikan Bahasa Inggris');
INSERT INTO mahasiswa VALUES('075410101','Uwais Al-Qarny',3.99,'Teknik Sipil');

INSERT INTO alamat(nim,nama_jalan,rt,rw,kode_desa,kode_kec,kode_kab,kode_prop) VALUES('075410099','Jln. Beo No.2','01','02','01','01','01','01');
INSERT INTO alamat(nim,nama_jalan,rt,rw,kode_desa,kode_kec,kode_kab,kode_prop) VALUES('075410100','Jln. Cenderawasih No.2','01','02','02','01','01','08');
INSERT INTO alamat(nim,nama_jalan,rt,rw,kode_desa,kode_kec,kode_kab,kode_prop) VALUES('075410101','Jln. Cucak Rowo No.2','01','02','02','01','01','09');