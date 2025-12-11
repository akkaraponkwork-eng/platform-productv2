/* SQL Script for MyIoTDB 
   Run on: Microsoft SQL Server Management Studio (SSMS)
*/

-- 1. เตรียม Environment (เริ่มที่ Master)
USE master;
GO

------------------------------------------------------------
-- 1. CREATE DATABASE (Clean Start)
------------------------------------------------------------
-- ตรวจสอบว่ามี Database อยู่ไหม ถ้ามีให้เตะคนออก (Single User) แล้วลบทิ้ง
IF EXISTS (SELECT name FROM sys.databases WHERE name = N'MyIoTDB')
BEGIN
    ALTER DATABASE MyIoTDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE MyIoTDB;
END
GO

-- สร้าง Database ใหม่
CREATE DATABASE MyIoTDB;
GO

-- เข้าใช้งาน Database
USE MyIoTDB;
GO

------------------------------------------------------------
-- 2. MASTER TABLE : site_detail
------------------------------------------------------------
CREATE TABLE site_detail (
    id INT IDENTITY(1,1) PRIMARY KEY,
    site_name VARCHAR(30),
    sensor_type VARCHAR(20),
    gateway VARCHAR(20),
    min_a FLOAT,
    max_a FLOAT,
    min_b FLOAT,
    max_b FLOAT,
    interval INT,
    skip BIT,
);
GO

------------------------------------------------------------
-- 3. SENSOR RAW TABLES
------------------------------------------------------------
CREATE TABLE o3_data (
    site_id INT NOT NULL,
    [timestamp] DATETIME2 NOT NULL,
    oz FLOAT,
    PRIMARY KEY (site_id, [timestamp]),
    FOREIGN KEY (site_id) REFERENCES site_detail(id)
);
GO

CREATE TABLE em320_th_data (
    site_id INT NOT NULL,
    [timestamp] DATETIME2 NOT NULL,
    temp FLOAT,
    humid FLOAT,
    battery FLOAT,
    PRIMARY KEY (site_id, [timestamp]),
    FOREIGN KEY (site_id) REFERENCES site_detail(id)
);
GO

CREATE TABLE em500_pt100_data (
    site_id INT NOT NULL,
    [timestamp] DATETIME2 NOT NULL,
    temp FLOAT,
    battery FLOAT,
    PRIMARY KEY (site_id, [timestamp]),
    FOREIGN KEY (site_id) REFERENCES site_detail(id)
);
GO

CREATE TABLE ws303_data (
    site_id INT NOT NULL,
    [timestamp] DATETIME2 NOT NULL,
    water_leak BIT,
    battery FLOAT,
    PRIMARY KEY (site_id, [timestamp]),
    FOREIGN KEY (site_id) REFERENCES site_detail(id)
);
GO

------------------------------------------------------------
-- 4. SAP BATCH & SUMMARY TABLES
------------------------------------------------------------
CREATE TABLE sap (
    batch_no VARCHAR(20) PRIMARY KEY,
    date_start CHAR(8),
    start_time CHAR(6),
    date_stop CHAR(8),
    stop_time CHAR(6),
    StartDateTime DATETIME2 NULL, 
    StopDateTime DATETIME2 NULL
);
GO

CREATE INDEX IX_sap_StartDateTime ON sap(StartDateTime);
GO

CREATE TABLE o3_sap (
    id INT IDENTITY(1,1) PRIMARY KEY,
    batch_no VARCHAR(20),
    site_id INT,
    oz_min FLOAT,
    oz_max FLOAT,
    oz_avg FLOAT,
    FOREIGN KEY (batch_no) REFERENCES sap(batch_no),
    FOREIGN KEY (site_id) REFERENCES site_detail(id)
);
GO

CREATE TABLE em500_pt100_sap (
    id INT IDENTITY(1,1) PRIMARY KEY,
    batch_no VARCHAR(20),
    site_id INT,
    temp_min FLOAT,
    temp_max FLOAT,
    temp_avg FLOAT,
    FOREIGN KEY (batch_no) REFERENCES sap(batch_no),
    FOREIGN KEY (site_id) REFERENCES site_detail(id)
);
GO

CREATE TABLE em320_th_sap (
    id INT IDENTITY(1,1) PRIMARY KEY,
    batch_no VARCHAR(20),
    site_id INT,
    temp_min FLOAT,
    temp_max FLOAT,
    temp_avg FLOAT,
    humid_min FLOAT,
    humid_max FLOAT,
    humid_avg FLOAT,
    FOREIGN KEY (batch_no) REFERENCES sap(batch_no),
    FOREIGN KEY (site_id) REFERENCES site_detail(id)
);
GO

------------------------------------------------------------
-- 5. WAREHOUSE + FG SUMMARY
------------------------------------------------------------
CREATE TABLE wh_details (
    id VARCHAR(10) PRIMARY KEY,
    [desc] VARCHAR(50),
    ax VARCHAR(5),
    site_id INT NULL FOREIGN KEY REFERENCES site_detail(id)
);
GO

CREATE TABLE invent_table (
    batch_no VARCHAR(20) PRIMARY KEY,
    wh_location VARCHAR(10),
    movein_date CHAR(8),
    movein_time CHAR(8),
    moveout_date CHAR(8),
    moveout_time CHAR(8),
    MoveInDateTime DATETIME2 NULL,
    MoveOutDateTime DATETIME2 NULL
);
GO

CREATE INDEX IX_invent_MoveInDateTime ON invent_table(MoveInDateTime);
GO

CREATE TABLE sap_fg_table (
    batch_no VARCHAR(20) PRIMARY KEY,
    wh_location VARCHAR(10),
    temp_min FLOAT,
    temp_max FLOAT,
    temp_avg FLOAT,
    humid_min FLOAT,
    humid_max FLOAT,
    humid_avg FLOAT
);
GO

------------------------------------------------------------
-- 6. TRIGGERS
------------------------------------------------------------
CREATE OR ALTER TRIGGER trg_after_insert_sap
ON sap
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE 
        @batch_no VARCHAR(20),
        @d_start CHAR(8), @t_start CHAR(6),
        @d_stop CHAR(8), @t_stop CHAR(6),
        @calc_start DATETIME2,
        @calc_stop DATETIME2;

    SELECT 
        @batch_no = batch_no,
        @d_start = date_start, @t_start = start_time,
        @d_stop = date_stop, @t_stop = stop_time
    FROM inserted;

    -- แปลง String เป็น DateTime2 (YYYYMMDD HHmmss -> YYYY-MM-DD HH:mm:ss)
    SET @calc_start = CONVERT(DATETIME2, STUFF(STUFF(@d_start,5,0,'-'),8,0,'-') + ' ' + STUFF(STUFF(@t_start,3,0,':'),6,0,':'), 120);
    SET @calc_stop = CONVERT(DATETIME2, STUFF(STUFF(@d_stop,5,0,'-'),8,0,'-') + ' ' + STUFF(STUFF(@t_stop,3,0,':'),6,0,':'), 120);

    -- อัปเดตฟิลด์ DateTime จริงกลับเข้าไปในตาราง
    UPDATE sap 
    SET StartDateTime = @calc_start, StopDateTime = @calc_stop
    WHERE batch_no = @batch_no;

    -- คำนวณค่าจาก O3
    INSERT INTO o3_sap (batch_no, site_id, oz_min, oz_max, oz_avg)
    SELECT @batch_no, s.id, MIN(oz), MAX(oz), AVG(oz)
    FROM o3_data d
    JOIN site_detail s ON s.id = d.site_id
    WHERE s.site_name IN ('o3-windup', 'o3-outside') AND d.[timestamp] BETWEEN @calc_start AND @calc_stop
    GROUP BY s.id;

    -- คำนวณค่าจาก PT100
    INSERT INTO em500_pt100_sap (batch_no, site_id, temp_min, temp_max, temp_avg)
    SELECT @batch_no, s.id, MIN(temp), MAX(temp), AVG(temp)
    FROM em500_pt100_data d
    JOIN site_detail s ON s.id = d.site_id
    WHERE s.site_name IN ('t-sat tank-01', 't-sat tank-02') AND d.[timestamp] BETWEEN @calc_start AND @calc_stop
    GROUP BY s.id;

    -- คำนวณค่าจาก Temp/Humid
    INSERT INTO em320_th_sap (batch_no, site_id, temp_min, temp_max, temp_avg, humid_min, humid_max, humid_avg)
    SELECT @batch_no, s.id, MIN(temp), MAX(temp), AVG(temp), MIN(humid), MAX(humid), AVG(humid)
    FROM em320_th_data d
    JOIN site_detail s ON s.id = d.site_id
    WHERE s.site_name IN ('th-windup', 'th-outside') AND d.[timestamp] BETWEEN @calc_start AND @calc_stop
    GROUP BY s.id;
END;
GO

CREATE OR ALTER TRIGGER trg_after_insert_invent
ON invent_table
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE 
        @batch_no VARCHAR(20), @wh_location VARCHAR(10),
        @d_in CHAR(8), @t_in CHAR(6),
        @d_out CHAR(8), @t_out CHAR(6),
        @calc_in DATETIME2, @calc_out DATETIME2;

    SELECT 
        @batch_no = batch_no, @wh_location = wh_location,
        @d_in = movein_date, @t_in = movein_time,
        @d_out = moveout_date, @t_out = moveout_time
    FROM inserted;

    SET @calc_in = CONVERT(DATETIME2, STUFF(STUFF(@d_in,5,0,'-'),8,0,'-') + ' ' + STUFF(STUFF(@t_in,3,0,':'),6,0,':'), 120);
    SET @calc_out = CONVERT(DATETIME2, STUFF(STUFF(@d_out,5,0,'-'),8,0,'-') + ' ' + STUFF(STUFF(@t_out,3,0,':'),6,0,':'), 120);

    UPDATE invent_table 
    SET MoveInDateTime = @calc_in, MoveOutDateTime = @calc_out
    WHERE batch_no = @batch_no;

    INSERT INTO sap_fg_table (batch_no, wh_location, temp_min, temp_max, temp_avg, humid_min, humid_max, humid_avg)
    SELECT @batch_no, @wh_location, MIN(temp), MAX(temp), AVG(temp), MIN(humid), MAX(humid), AVG(humid)
    FROM em320_th_data d
    JOIN site_detail s ON s.id = d.site_id
    JOIN wh_details w ON w.site_id = s.id
    WHERE @wh_location LIKE w.id + '%' AND d.[timestamp] BETWEEN @calc_in AND @calc_out;
END;
GO

------------------------------------------------------------
-- 7. DATA SEEDING (ข้อมูลตั้งต้น)
------------------------------------------------------------
INSERT INTO site_detail (site_name, sensor_type, gateway, min_a, max_a,min_b, max_b, interval, skip) VALUES
('TH-Windup', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-Letoff', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-GF WH-01', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-GF WH-02', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-Rewind', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-Dip Packing', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-Chem Room', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-Chem WH', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-FG Zone A', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-FG Zone B', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-FG Zone C', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-FG Zone D', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-FG Zone E', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-LAB-01', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-LAB-02', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-Dip Prep floor2', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-Dip Prep floor3', 'EM320-TH', 1, 0, 100,0, 0, 10, 0),
('TH-Outside', 'EM300', 1, 0, 100,0, 0, 10, 0),
('T-SAT Tank-01', 'EM500-PT100', 1, 0, 100,0, 0, 10, 0),
('T-SAT Tank-02', 'EM500-PT100', 1, 0, 100,0, 0, 10, 0),
('O3-Windup', 'O3', 1, 0, 100,0, 0, 10, 0),
('O3-Outside', 'O3', 1, 0, 100,0, 0, 10, 0),
('TH-Open WH', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-Yarn YW', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-Yarn YE', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-Yarn Raw', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-Server-01', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-Server-02', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-TW-01', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-TW-02', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-TW-03', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-TW-04', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-INTMD-01', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-INTMD-02', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-WV-01', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('TH-WV-02', 'EM320-TH', 2, 0, 100,0, 0, 10, 0),
('WL-Server-01', 'WS303', 2, 0, 100,0, 0, 10, 0),
('WL-Server-02', 'WS303', 2, 0, 100,0, 0, 10, 0),
('WL-Server-03', 'WS303', 2, 0, 100,0, 0, 10, 0),
('WL-Server-04', 'WS303', 2, 0, 100,0, 0, 10, 0);
GO

INSERT INTO wh_details([id],[desc],[ax],[site_id]) VALUES
('4A','FG Zone A','A',9),
('4B','FG Zone B','B',10),
('4C','FG Zone C','C',11),
('4D','FG Zone D','D',12),
('4E','FG Zone E','E',13);
GO