use CBS;
go

/*
Notes:
Be aware: IT DELETES !!!!
errors on empty directories
*/

--DECLARE @filedir varchar(1000) = 'D:\NLDATA\CBS\test';
DECLARE @filedir varchar(1000) = 'D:\NLDATA\CBS\CBS20210601'; -- <-- CHANGE
DECLARE @debug varchar(1) = 'N' --Y,N,I
DECLARE @quote varchar(max) = '''';
DECLARE @crlf varchar(max) = CHAR(13)+CHAR(10);
DECLARE @doscommand varchar(8000);
DECLARE @result int; 
DECLARE @CBSDirectoryDateTime datetime;
DECLARE @CBSDirectoryName varchar(1000);
DECLARE @CBSFileName varchar(1000);
DECLARE @sql nvarchar(max) ='';
DECLARE @msg nvarchar(max) = '';

DROP TABLE IF EXISTS Tbl_CBS_ID_files;
CREATE TABLE Tbl_CBS_ID_files ( 
  CBSDirectoryName varchar(1000) NULL
, CBSDirectoryDateTime datetime NULL
, CBSFileName varchar(1000) NULL
, CBSFileDateTime datetime NULL
, CBSFileSize varchar(1000) NULL
);

DROP TABLE IF EXISTS Tbl_CBS_ID_dirs;
CREATE TABLE Tbl_CBS_ID_dirs ( Line VARCHAR(512));


SET @doscommand = 'dir '+@filedir+ ' /TC';
IF @debug = 'Y' 
	BEGIN
  SET @msg = @doscommand
	IF LEN(@msg) > 2047
		PRINT @msg;
	ELSE
		RAISERROR (@msg, 0, 1) WITH NOWAIT; 
	END;

INSERT INTO Tbl_CBS_ID_dirs
EXEC @result = MASTER..xp_cmdshell   @doscommand ;
IF (@result = 0)  
   PRINT 'Success'  
ELSE  
   PRINT 'Failure'  
;

DELETE
FROM   Tbl_CBS_ID_dirs
WHERE  1=1
--AND Line NOT LIKE '[0-9][0-9]/[0-9][0-9]/[0-9][0-9][0-9][0-9] %'
AND (Line LIKE '%.' OR Line is null)
OR Line NOT LIKE'%<DIR>%'
;

DECLARE CBS_Dir_Cursor CURSOR 
  FAST_FORWARD
  FOR 
SELECT
  CBSDirectoryDateTime = CONVERT(DATETIME2,LEFT(Line,17)+':00',103)
, CBSDirectoryName = REVERSE( LEFT(REVERSE(Line),CHARINDEX(' ',REVERSE(line))-1 ) )
FROM Tbl_CBS_ID_dirs
OPEN CBS_Dir_Cursor  
FETCH NEXT FROM CBS_Dir_Cursor
INTO @CBSDirectoryDateTime, @CBSDirectoryName; 
WHILE @@FETCH_STATUS = 0  
BEGIN  
  --Begin processing directories

  PRINT @CBSDirectoryName;

  DROP TABLE IF EXISTS #CommandShell;
  CREATE TABLE #CommandShell ( Line VARCHAR(512));
  SET @doscommand = 'dir '+@filedir+'\'+@CBSDirectoryName+'\*.csv /TC | FIND "csv"';
  IF @debug = 'Y' 
	  BEGIN
    SET @msg = @doscommand
	  IF LEN(@msg) > 2047
		  PRINT @msg;
	  ELSE
		  RAISERROR (@msg, 0, 1) WITH NOWAIT; 
	  END;

  INSERT INTO #CommandShell
  EXEC @result = MASTER..xp_cmdshell   @doscommand ;
  IF (@result = 0)  
     PRINT 'Success'  
  ELSE  
     PRINT 'Failure'  
  ;

  INSERT INTO Tbl_CBS_ID_files
  SELECT
    @CBSDirectoryName AS CBSDirectoryName
  , @CBSDirectoryDateTime AS CBSDirectoryDateTime
  , CBSFileName = REVERSE( LEFT(REVERSE(Line),CHARINDEX(' ',REVERSE(Line))-1 ) )
  , CBSFileDateTime = FORMAT(CONVERT(DATETIME2,LEFT(Line,17)+':00',103), 'dd-MMM-yyyy HH:mm:ss')
  , CBSFileSize = REPLACE(TRIM(SUBSTRING(Line,18,19)),'.','')
  FROM #CommandShell
  WHERE 1=1
  AND REVERSE( LEFT(REVERSE(Line),CHARINDEX(' ',REVERSE(Line))-1 ) ) IS NOT NULL

  DECLARE CBS_File_Cursor CURSOR 
    FAST_FORWARD
    FOR 
  SELECT
    CBSFileName = REVERSE( LEFT(REVERSE(Line),CHARINDEX(' ',REVERSE(Line))-1 ) )
  FROM #CommandShell
  WHERE 1=1
  AND CONVERT(bigint,REPLACE(TRIM(SUBSTRING(Line,18,19)),'.',''))>4 --avoid CategoryGroups with only 2 double quotes in it
  OPEN CBS_File_Cursor  
  FETCH NEXT FROM CBS_File_Cursor
  INTO @CBSFileName; 
  WHILE @@FETCH_STATUS = 0  
  BEGIN  
    DROP TABLE IF EXISTS [tmp_One_Column];
    CREATE TABLE [tmp_One_Column](
      OneColumn varchar(max) NULL
    );
    SET @sql = '
    BULK INSERT [tmp_One_Column]
    FROM '+@quote+@filedir+'\'+@CBSDirectoryName+'\'+@CBSFileName+@quote+'
    WITH (
      CODEPAGE = '+@quote+'RAW'+@quote+'
    , DATAFILETYPE = '+@quote+'char'+@quote+'
    , ROWTERMINATOR = '+@quote+'\n'+@quote+'
    , FIRSTROW = 1
    , LASTROW = 1
    , TABLOCK
    );'
    IF @debug = 'Y' 
		BEGIN
      SET @msg = @sql
		  IF LEN(@msg) > 2047
			  PRINT @msg;
		  ELSE
			  RAISERROR (@msg, 0, 1) WITH NOWAIT; 
	  END;
	  EXEC (@sql);
    --drop table
    SET @sql = 'DROP TABLE IF EXISTS [tblCBS_'+@CBSDirectoryName+'_'+REPLACE(@CBSFileName,'.csv','')+'];'
    IF @debug = 'Y' 
		BEGIN
      SET @msg = @sql
		  IF LEN(@msg) > 2047
			  PRINT @msg;
		  ELSE
			  RAISERROR (@msg, 0, 1) WITH NOWAIT; 
	  END;
    EXEC (@sql);
    --create table
    SELECT @sql = 'CREATE TABLE [tblCBS_'+@CBSDirectoryName+'_'+REPLACE(@CBSFileName,'.csv','')+'] ('+@crlf+'['+REPLACE(SUBSTRING(t1.OneColumn,2,LEN(t1.OneColumn)-2),'","','] varchar(max) NULL,'+@crlf+'[')+'] varchar(max) NULL);'
    FROM [tmp_One_Column] t1
    ;
    IF @debug = 'Y' 
		BEGIN
      SET @msg = @sql
		  IF LEN(@msg) > 2047
			  PRINT @msg;
		  ELSE
			  RAISERROR (@msg, 0, 1) WITH NOWAIT; 
	  END;
    EXEC (@sql);
    --load data
    SET @sql = '
    BULK INSERT [tblCBS_'+@CBSDirectoryName+'_'+REPLACE(@CBSFileName,'.csv','')+']
    FROM '+@quote+@filedir+'\'+@CBSDirectoryName+'\'+@CBSFileName+@quote+'
    WITH (
      CODEPAGE = '+@quote+'RAW'+@quote+'
    , FORMAT = '+@quote+'CSV'+@quote+'
    --, DATAFILETYPE = '+@quote+'char'+@quote+'
    , ROWTERMINATOR = '+@quote+'\n'+@quote+'
    , FIRSTROW = 2
    , TABLOCK
    );'
    IF @debug = 'Y' 
		BEGIN
      SET @msg = @sql
		  IF LEN(@msg) > 2047
			  PRINT @msg;
		  ELSE
			  RAISERROR (@msg, 0, 1) WITH NOWAIT; 
	  END;
    EXEC (@sql);
    FETCH NEXT FROM CBS_File_Cursor   
    INTO @CBSFileName; 
  END
  CLOSE CBS_File_Cursor;  
  DEALLOCATE CBS_File_Cursor;  

  --End processing directory
  FETCH NEXT FROM CBS_Dir_Cursor   
  INTO @CBSDirectoryDateTime, @CBSDirectoryName; 
END
CLOSE CBS_Dir_Cursor;  
DEALLOCATE CBS_Dir_Cursor;  

--drop 1 line table
DROP TABLE IF EXISTS [tmp_One_Column];
