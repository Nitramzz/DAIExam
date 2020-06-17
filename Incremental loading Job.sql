USE [msdb]
GO

/****** Object:  Job [Incremental_loading]    Script Date: 28-May-20 17:19:23 ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 28-May-20 17:19:23 ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Incremental_loading', 
		@enabled=1, 
		@notify_level_eventlog=3, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'MICHAL-XPS15\minco', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [RoomDimension loading from the source database]    Script Date: 28-May-20 17:19:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'RoomDimension loading from the source database', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'--FIRST QUERY TO RUN
-- RoomDimension loading from the source database
INSERT INTO SEPstagingDB.staging.RoomDimensionStaging(RoomID, RoomName, Location)
SELECT sensor.dbo.Room.RoomID,
       sensor.dbo.Room.Name,
       sensor.dbo.Room.Location

FROM sensor.dbo.Room
WHERE NOT EXISTS (
    SELECT *
    FROM SEPstagingDB.staging.RoomDimensionStaging
    WHERE sensor.dbo.Room.RoomID = SEPstagingDB.staging.RoomDimensionStaging.RoomID
    AND   sensor.dbo.Room.Name = SEPstagingDB.staging.RoomDimensionStaging.RoomName);
--END OF FIRST QUERY!!', 
		@database_name=N'SEPstagingDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [MainFact loading data from source database]    Script Date: 28-May-20 17:19:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'MainFact loading data from source database', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
--SECOND QUERY TO RUN
/*
 MainFact loading data from source database. WIPE THE FACT BEFORE USING THIS TO PREVENT "TEMP" DUPLICATES!!!
*/
DECLARE @@LatestDate DATETIME2;
SELECT @@LatestDate = MAX(SEPstagingDB.staging.MainFactStaging.Date)
FROM SEPstagingDB.staging.MainFactStaging

INSERT INTO SEPstagingDB.staging.MainFactStaging(RoomID, Date, TEMP_value)
SELECT
       sensor.dbo.Room.RoomID,
       sensor.dbo.Temperature.Date,
       sensor.dbo.Temperature.TEMP_value

FROM (sensor.dbo.Room  JOIN sensor.dbo.TemperatureList ON sensor.dbo.Room.RoomID = sensor.dbo.TemperatureList.ROOM_ID)JOIN sensor.dbo.Temperature ON sensor.dbo.TemperatureList.TEMP_ID = sensor.dbo.Temperature.TEMP_ID
WHERE sensor.dbo.Temperature.Date > @@LatestDate; -- <-- comment for the initial load
--END OF SECOND QUERY TO RUN!!!
', 
		@database_name=N'SEPstagingDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [MainFact loading rest of the data]    Script Date: 28-May-20 17:19:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'MainFact loading rest of the data', 
		@step_id=3, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
--THIRD QUERY TO RUN
UPDATE SEPstagingDB.staging.MainFactStaging
SET MainFactStaging.CO2_value = sensor.dbo.CO2.CO2_value
FROM (sensor.dbo.Room  JOIN sensor.dbo.CO2List ON Room.RoomID = CO2List.ROOM_ID)JOIN sensor.dbo.CO2 ON CO2List.CO2_ID = CO2.CO2ID
WHERE MainFactStaging.Date = sensor.dbo.CO2.Date;


UPDATE SEPstagingDB.staging.MainFactStaging
SET MainFactStaging.HUMDT_value = sensor.dbo.Humidity.HUM_value
FROM (sensor.dbo.Room  JOIN sensor.dbo.HumidityList ON Room.RoomID = HumidityList.ROOM_ID)JOIN sensor.dbo.Humidity ON HumidityList.HUM_ID = Humidity.HUM_ID
WHERE MainFactStaging.Date = sensor.dbo.Humidity.Date;


UPDATE SEPstagingDB.staging.MainFactStaging
SET MainFactStaging.Spinning = sensor.dbo.Servo.Spinning
FROM (sensor.dbo.Room  JOIN sensor.dbo.ServoList ON Room.RoomID = ServoList.ROOM_ID)JOIN sensor.dbo.Servo ON ServoList.SERV_ID = Servo.SERV_ID
WHERE MainFactStaging.Date = sensor.dbo.Servo.Date;


UPDATE SEPstagingDB.staging.MainFactStaging
SET MainFactStaging.R_ID = SEPstagingDB.staging.RoomDimensionStaging.R_ID
FROM SEPstagingDB.staging.RoomDimensionStaging
WHERE RoomDimensionStaging.RoomID = MainFactStaging.RoomID;
--END OF THIRD QUERY TO RUN!!!
', 
		@database_name=N'SEPstagingDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [DateDimension new date loading]    Script Date: 28-May-20 17:19:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'DateDimension new date loading', 
		@step_id=4, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
--FOURTH QUERY TO RUN
INSERT INTO SEPstagingDB.staging.DateDimension(WeekDayName, MonthName, Date, Time)
SELECT DISTINCT
       DATENAME(weekday ,SEPstagingDB.staging.MainFactStaging.Date),
       DATENAME(month,SEPstagingDB.staging.MainFactStaging.Date),
       SEPstagingDB.staging.MainFactStaging.Date,
       cast(SEPstagingDB.staging.MainFactStaging.Date as time(0))

FROM SEPstagingDB.staging.MainFactStaging
WHERE NOT EXISTS (
    SELECT *
    FROM SEPstagingDB.staging.DateDimension
    WHERE SEPstagingDB.staging.MainFactStaging.Date = SEPstagingDB.staging.DateDimension.Date);
--END OF FOURTH QUERY
', 
		@database_name=N'SEPstagingDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Updating the DateID's in MainFact (END OF STAGING JOBS)]    Script Date: 28-May-20 17:19:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Updating the DateID''s in MainFact (END OF STAGING JOBS)', 
		@step_id=5, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
--FIFTH & FINAL QUERY TO RUN
UPDATE SEPstagingDB.staging.MainFactStaging
SET MainFactStaging.D_ID = SEPstagingDB.staging.DateDimension.D_ID
FROM SEPstagingDB.staging.DateDimension
WHERE MainFactStaging.Date = SEPstagingDB.staging.DateDimension.Date
--END OF THE FIFTH & FINAL QUERY
', 
		@database_name=N'SEPstagingDB', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [RoomDimension loading from the staging database]    Script Date: 28-May-20 17:19:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'RoomDimension loading from the staging database', 
		@step_id=6, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'--FIRST QUERY TO RUN

DECLARE @@LatestDate DATETIME2;
SELECT @@LatestDate = MAX(DWSEP4.dbo.DateDimension.Date)
FROM DWSEP4.dbo.DateDimension

--REMEMBER TO WIPE THE DATA FROM THE DW IN CASE YOU''RE DOING NEW LOAD, TO PREVENT HAVING DUPLICATE VALUES!!!

--RoomDimension loading from the staging database

INSERT INTO DWSEP4.dbo.RoomDimension(R_ID, RoomName, Location, RoomID)
SELECT SEPstagingDB.staging.RoomDimensionStaging.R_ID,
       SEPstagingDB.staging.RoomDimensionStaging.RoomName,
       SEPstagingDB.staging.RoomDimensionStaging.Location,
       SEPstagingDB.staging.RoomDimensionStaging.RoomID

FROM SEPstagingDB.staging.RoomDimensionStaging
WHERE NOT EXISTS(
    SELECT *
    FROM  DWSEP4.dbo.RoomDimension
    WHERE SEPstagingDB.staging.RoomDimensionStaging.RoomID = DWSEP4.dbo.RoomDimension.RoomID
    AND   SEPstagingDB.staging.RoomDimensionStaging.RoomName = DWSEP4.dbo.RoomDimension.RoomName);

--END OF FIRST QUERY', 
		@database_name=N'DWSEP4', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [DateDimension loading from the staging database]    Script Date: 28-May-20 17:19:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'DateDimension loading from the staging database', 
		@step_id=7, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'--SECOND QUERY TO RUN

--DateDimension loading from the staging database
DECLARE @@LatestDate DATETIME2;
SELECT @@LatestDate = MAX(DWSEP4.dbo.DateDimension.Date)
FROM DWSEP4.dbo.DateDimension

INSERT INTO DWSEP4.dbo.DateDimension(Date, WeekDayName, MonthName, Time, D_ID)
SELECT SEPstagingDB.staging.DateDimension.Date,
       SEPstagingDB.staging.DateDimension.WeekDayName,
       SEPstagingDB.staging.DateDimension.MonthName,
       SEPstagingDB.staging.DateDimension.Time,
       SEPstagingDB.staging.DateDimension.D_ID
FROM SEPstagingDB.staging.DateDimension
WHERE @@LatestDate < SEPstagingDB.staging.DateDimension.Date -- <-- comment this out when doing initial loading
--END OF DateDimension LOADING

--END OF SECOND QUERY
', 
		@database_name=N'DWSEP4', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [MainFact loading from the staging database]    Script Date: 28-May-20 17:19:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'MainFact loading from the staging database', 
		@step_id=8, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'--THIRD QUERY TO RUN

--MainFact loading from the staging database
DECLARE @@LatestDate DATETIME2;
SELECT @@LatestDate = DWSEP4.dbo.ChangeDimension.lastUpdated
FROM DWSEP4.dbo.ChangeDimension

INSERT INTO DWSEP4.dbo.MainFact(R_ID, D_ID, CO2_value, TEMP_value, HUMDT_value, Spinning)
SELECT SEPstagingDB.staging.MainFactStaging.R_ID,
       SEPstagingDB.staging.MainFactStaging.D_ID,
       SEPstagingDB.staging.MainFactStaging.CO2_value,
       SEPstagingDB.staging.MainFactStaging.TEMP_value,
       SEPstagingDB.staging.MainFactStaging.HUMDT_value,
       SEPstagingDB.staging.MainFactStaging.Spinning
FROM SEPstagingDB.staging.MainFactStaging
WHERE @@LatestDate < SEPstagingDB.staging.MainFactStaging.Date -- <--- comment this out for initial load

--END OF THIRD QUERY
', 
		@database_name=N'DWSEP4', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Update the ChangeDimension]    Script Date: 28-May-20 17:19:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Update the ChangeDimension', 
		@step_id=9, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
--FOURTH QUERY TO RUN

DECLARE @@LatestDate DATETIME2;
SELECT @@LatestDate = MAX(DWSEP4.dbo.DateDimension.Date)
FROM DWSEP4.dbo.DateDimension

UPDATE DWSEP4.dbo.ChangeDimension
SET ChangeDimension.lastUpdated = @@LatestDate
FROM DWSEP4.dbo.DateDimension

--END OF FOURTH QUERY
', 
		@database_name=N'DWSEP4', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Copy the DateID's from DateDimension]    Script Date: 28-May-20 17:19:23 ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Copy the DateID''s from DateDimension', 
		@step_id=10, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'TSQL', 
		@command=N'
--FIFTH AND FINAL QUERY

UPDATE SEPstagingDB.staging.MainFactStaging
SET MainFactStaging.D_ID = SEPstagingDB.staging.DateDimension.D_ID
FROM SEPstagingDB.staging.DateDimension
WHERE MainFactStaging.Date = SEPstagingDB.staging.DateDimension.Date

--END OF FIFTH AND FINAL QUERY
', 
		@database_name=N'DWSEP4', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Staging_incremental_load', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20200528, 
		@active_end_date=99991231, 
		@active_start_time=150000, 
		@active_end_time=235959, 
		@schedule_uid=N'31a8407d-db72-42ac-b815-e053e512f508'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
GO

