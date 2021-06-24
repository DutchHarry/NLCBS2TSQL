USE CBS;
GO

/*

*/


DECLARE @debug varchar(1) = 'N' --Y,N,I
DECLARE @quote varchar(max) = '''';
DECLARE @crlf varchar(max) = CHAR(13)+CHAR(10);
DECLARE @datatablename varchar(MAX) = ''
DECLARE @datacolumns varchar(MAX)='';
DECLARE @dimensioncolumns varchar(max)='';
DECLARE @alldimensioncolumns varchar(max)='';
DECLARE @catdimensionjoins varchar(max)='';
DECLARE @catdimensioncolumns varchar(max)='';
DECLARE @joincolumns varchar(max)='';
DECLARE @CBSDimensionName varchar(max)='';
DECLARE @CBSTableID varchar(max)='';
DECLARE @CBSID varchar(max)='';
DECLARE @sql nvarchar(max)='';  --needs to be nvarchar(max): with varchar(max) truncation occurs in last concatenation
DECLARE @msg nvarchar(max) = ''; -- see @sql

DECLARE CBS_ID_Cursor CURSOR 
  FAST_FORWARD
  FOR 
SELECT distinct
  SUBSTRING(t.name,8,LEN(t.name)-5-7)
FROM sysobjects t
WHERE 1=1
and   t.name LIKE '%tblCBS_%_data'
-- beyond 8000 chars
--AND SUBSTRING(t.name,8,LEN(t.name)-5-7) IN ('20190NED','60048ned','70072ned','70097NED','70221NED','70227ned','70904ned','70942ned','71230ned','71718ned','71778ned','7467','80418ned','80485ned','80781ned','80783eng','81238ned','81628NED','81903NED','82270NED','82341NED','82436NED','82829NED','82931NED','82971NED','83005NED','83220NED','83304NED','83487NED','83558NED','83749NED','83765NED','84463NED','84583NED','84799NED')
OPEN CBS_ID_Cursor  
FETCH NEXT FROM CBS_ID_Cursor
INTO @CBSID; 
WHILE @@FETCH_STATUS = 0  
BEGIN  
  -- BEGIN cbs id PROCESSING
  SET @CBSTableID = @CBSID;
  SET @datatablename = 'tblCBS_'+@CBSID+'_data';

  SET @msg = @CBSID+' Started!';
  RAISERROR (@msg, 0, 1) WITH NOWAIT; 

  SET @datacolumns = ''
  SELECT 
    @datacolumns = @datacolumns+@crlf+COALESCE(
    --trim spaces and '.' from real data columns; converion to int or float stil needed!
    CASE WHEN isnumeric(REVERSE(REPLACE(SUBSTRING(REVERSE(c.name),1,CHARINDEX('_',REVERSE(c.name))),'_','')))=1 THEN ', TRIM(REPLACE([data].['+c.name+'],'' .'',''  '')) AS [data_'+c.name+']' ELSE ', [data].[' + c.name+'] AS [data_'+c.name+']' END
    ,'')  
  FROM sysobjects t
  INNER JOIN syscolumns c
  ON c.id = t.id
  WHERE 1=1
  and   t.name = @datatablename 
  ORDER BY c.colid
  ;
	IF @debug = 'Y' 
		BEGIN
    SET @msg = @datacolumns
		IF LEN(@msg) > 2047
			PRINT @msg;
		ELSE
			RAISERROR (@msg, 0, 1) WITH NOWAIT; 
		END;

  SET @joincolumns = ''
  SELECT 
  @joincolumns = COALESCE(@joincolumns +@crlf+'LEFT JOIN [tblCBS_'+@CBSTableID+'_' + c.name+'] '+c.name+@crlf+'ON data.['+c.name+'] = ['+c.name+'].[Key]', 'LEFT JOIN [tblCBS_'+@CBSTableID+'_'+c.name+'] '+c.name+@crlf+'ON data.['+c.name+'] = ['+c.name+'].[Key]') 
  FROM sysobjects t
  INNER JOIN syscolumns c
  ON c.id = t.id
  WHERE 1=1
  and   t.name = @datatablename
  AND c.name<>'ID'
  AND isnumeric(REVERSE(REPLACE(SUBSTRING(REVERSE(c.name),1,CHARINDEX('_',REVERSE(c.name))),'_','')))<>1
  ORDER BY c.colid
  ;
	IF @debug = 'Y' 
		BEGIN
    SET @msg = @joincolumns
		IF LEN(@msg) > 2047
			PRINT @msg;
		ELSE
			RAISERROR (@msg, 0, 1) WITH NOWAIT; 
		END;

  SET @dimensioncolumns = ''
  SET @catdimensionjoins = ''
  SET @catdimensioncolumns = ''

  DECLARE CBS_Dimension_Cursor CURSOR 
    FAST_FORWARD
    FOR 
  SELECT 
    c.name
  FROM sysobjects t
  INNER JOIN syscolumns c
  ON c.id = t.id
  WHERE 1=1
  --and   t.xtype = 'u'
  and   t.name = @datatablename --'Tbl_Dirs_Data'
  AND c.name<>'ID'
  AND isnumeric(REVERSE(REPLACE(SUBSTRING(REVERSE(c.name),1,CHARINDEX('_',REVERSE(c.name))),'_','')))<>1
  --AND SUBSTRING(c.name,1,CHARINDEX('_',REVERSE(c.name)
  ORDER BY c.colid
  ;
  OPEN CBS_Dimension_Cursor  
  FETCH NEXT FROM CBS_Dimension_Cursor
  INTO @CBSDimensionName; 
  WHILE @@FETCH_STATUS = 0  
  BEGIN  

    SELECT @dimensioncolumns = COALESCE(@dimensioncolumns+@crlf+', ['+@CBSDimensionName+'].['+c.name+'] AS '+@CBSDimensionName+'_'+c.name, ', ['+@CBSDimensionName+'].['+c.name+'] AS '+@CBSDimensionName+'_'+c.name) 
    FROM sysobjects t
    INNER JOIN syscolumns c
    ON c.id = t.id
    WHERE 1=1
    and   t.name = 'tblCBS_'+@CBSTableID+'_'+@CBSDimensionName 
    ORDER BY c.colid
    ;
    IF OBJECT_ID('tblCBS_'+@CBSID+'_CategoryGroups', 'U') IS NOT NULL  
    BEGIN
      --link categorygroup
      SELECT 
       @catdimensionjoins =  @catdimensionjoins 
       + COALESCE('
       LEFT JOIN [tblCBS_'+@CBSTableID+'_CategoryGroups] c1_'+@CBSDimensionName+'
       ON ['+@CBSDimensionName+'].[CategoryGroupID]=[c1_'+@CBSDimensionName+'].[ID]
       LEFT JOIN [tblCBS_'+@CBSTableID+'_CategoryGroups] c2_'+@CBSDimensionName+'
       ON [c1_'+@CBSDimensionName+'].[ParentID]=[c2_'+@CBSDimensionName+'].[ID]
       LEFT JOIN [tblCBS_'+@CBSTableID+'_CategoryGroups] c3_'+@CBSDimensionName+'
       ON [c2_'+@CBSDimensionName+'].[ParentID]=[c3_'+@CBSDimensionName+'].[ID]', '')
      , @catdimensioncolumns =  @catdimensioncolumns 
      + COALESCE('
      , [c1_'+@CBSDimensionName+'].[ID] AS [c1_'+@CBSDimensionName+'_ID]
      , [c1_'+@CBSDimensionName+'].[DimensionKey] AS [c1_'+@CBSDimensionName+'_DimensionKey]
      , [c1_'+@CBSDimensionName+'].[Title] AS [c1_'+@CBSDimensionName+'_Title]
      , [c1_'+@CBSDimensionName+'].[Description] AS [c1_'+@CBSDimensionName+'_Description]
      , [c1_'+@CBSDimensionName+'].[ParentID] AS [c1_'+@CBSDimensionName+'_ParentID]
      , [c2_'+@CBSDimensionName+'].[ID] AS [c2_'+@CBSDimensionName+'_ID]
      , [c2_'+@CBSDimensionName+'].[DimensionKey] AS [c12'+@CBSDimensionName+'_DimensionKey]
      , [c2_'+@CBSDimensionName+'].[Title] AS [c2_'+@CBSDimensionName+'_Title]
      , [c2_'+@CBSDimensionName+'].[Description] AS [c2_'+@CBSDimensionName+'_Description]
      , [c2_'+@CBSDimensionName+'].[ParentID] AS [c2_'+@CBSDimensionName+'_ParentID]
      , [c3_'+@CBSDimensionName+'].[ID] AS [c3_'+@CBSDimensionName+'_ID]
      , [c3_'+@CBSDimensionName+'].[DimensionKey] AS [c3_'+@CBSDimensionName+'_DimensionKey]
      , [c3_'+@CBSDimensionName+'].[Title] AS [c3_'+@CBSDimensionName+'_Title]
      , [c3_'+@CBSDimensionName+'].[Description] AS [c3_'+@CBSDimensionName+'_Description]
      , [c3_'+@CBSDimensionName+'].[ParentID] AS [c3_'+@CBSDimensionName+'_ParentID]
      '
      +@crlf, '')

      FROM sysobjects t
      INNER JOIN syscolumns c
      ON c.id = t.id
      WHERE 1=1
      AND t.name = 'tblCBS_'+@CBSTableID+'_'+@CBSDimensionName 
      AND c.name = 'CategoryGroupID'
      ORDER BY c.colid
      ;
    END

    FETCH NEXT FROM CBS_Dimension_Cursor   
    INTO @CBSDimensionName; 
  END
  CLOSE CBS_Dimension_Cursor;  
  DEALLOCATE CBS_Dimension_Cursor;  

	IF @debug = 'Y' 
		BEGIN
    SET @msg = @catdimensionjoins
		IF LEN(@msg) > 2047
			PRINT @msg;
		ELSE
			RAISERROR (@msg, 0, 1) WITH NOWAIT; 
		END;
	IF @debug = 'Y' 
		BEGIN
    SET @msg = @catdimensioncolumns
		IF LEN(@msg) > 2047
			PRINT @msg;
		ELSE
			RAISERROR (@msg, 0, 1) WITH NOWAIT; 
		END;

  -- create view with all dimensions and columns
  SET @sql = 'CREATE OR ALTER VIEW Vw_'+@CBSTableID+' AS
  SELECT 
   '+substring(@datacolumns,4,100000)+@dimensioncolumns+@catdimensioncolumns+'
  FROM ['+@datatablename+'] data'+@joincolumns+@catdimensionjoins+';'


	IF @debug = 'Y' 
		BEGIN
    SET @msg = @sql
		IF LEN(@msg) > 2047
			PRINT @msg;
		ELSE
			RAISERROR (@msg, 0, 1) WITH NOWAIT; 
		END;

  EXEC (@sql);

  SET @msg = @CBSID+' Done!';
  RAISERROR (@msg, 0, 1) WITH NOWAIT; 

  -- END cbs id PROCESSING
  FETCH NEXT FROM CBS_ID_Cursor   
  INTO @CBSID; 
END
CLOSE CBS_ID_Cursor;  
DEALLOCATE CBS_ID_Cursor;  
