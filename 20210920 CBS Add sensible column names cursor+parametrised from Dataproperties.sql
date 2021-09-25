USE CBS;
GO
/*
Add sensible column names from Dataproperties
dynamic
*/

DECLARE @debug varchar(1) = 'N' --Y,N,I
DECLARE @quote varchar(max) = '''';
DECLARE @crlf varchar(max) = CHAR(13)+CHAR(10);
DECLARE @datacolumns varchar(max)=''
DECLARE @sql nvarchar(max)=''
DECLARE @dsql nvarchar(max)=''
DECLARE @CBSID varchar(100) = '84721NED'  --value for testing, gets value from cursor

DECLARE CBSID_Cursor CURSOR 
  FAST_FORWARD
  FOR 
  SELECT t.v FROM (VALUES 
  -- Views than benefit from 'sensible names'
  ('37144gin')
--, ('84992NED') --could use other names from Topicgroup
, ('37194')
, ('60049GIN')
, ('70174NED')
, ('70785NED')
, ('70931NED')
, ('71110NED')
, ('71377NED')
, ('71539ned')
, ('72014ned')
, ('7249PCR')
, ('7427PCR')
, ('80397ned')
, ('80868ned')
, ('81008ned')
, ('81498ned')
, ('82000NED')
, ('82496NED')
, ('82949NED')
, ('83287NED')
, ('83553NED')
, ('83859NED')
, ('84378NED')
, ('84721NED')
, ('84929NED')
, ('GIR96')
, ('gir97')
) AS t(v);
OPEN CBSID_Cursor  
FETCH NEXT FROM CBSID_Cursor
INTO @CBSID; 
WHILE @@FETCH_STATUS = 0  
BEGIN  
  --BEING DO SOMETHING WITHIN CURSOR
-- basically replace every quote ['] with ['+@quote+']
SET @dsql = '
DECLARE @crlf varchar(5) = CHAR(13)+CHAR(10); --easiest to declare within dynamic string
SET @datacolumns = '+@quote+''+@quote+'; --needs reset otherwise carries on values from previous
SELECT @datacolumns = @datacolumns +COALESCE(@crlf+'+@quote+' ,'+@quote+'+
  c.name+'+@quote+' AS ['+@quote+'+
  COALESCE(
    (
      SELECT c.name+'+@quote+' '+@quote+'+p2.[Title] 
      FROM [tblCBS_'+@CBSID+'_Dataproperties] p1 
      LEFT JOIN [tblCBS_'+@CBSID+'_Dataproperties] p2
      ON p1.ID = p2.[ID]+2
      WHERE 1=1
      AND '+@quote+'data_'+@quote+'+p1.[Key] = c.name
      AND ('+@quote+'data_'+@quote+'+p1.[Key] LIKE '+@quote+'data_Naam_%'+@quote+' OR '+@quote+'data_'+@quote+'+p1.[Key] LIKE '+@quote+'data_Omschrijving_%'+@quote+' )
    )
    , 
    (
      SELECT c.name+'+@quote+' '+@quote+'+p2.[Title] 
      FROM [tblCBS_'+@CBSID+'_Dataproperties] p1 
      LEFT JOIN [tblCBS_'+@CBSID+'_Dataproperties] p2
      ON p1.ID = p2.[ID]+1
      WHERE 1=1
      AND '+@quote+'data_'+@quote+'+p1.[Key] = c.name
      AND '+@quote+'data_'+@quote+'+p1.[Key] LIKE '+@quote+'data_Code_%'+@quote+'
    )
    , 
    c.name
  )+'+@quote+']'+@quote+'
,'+@quote+''+@quote+')
FROM sysobjects t
INNER JOIN syscolumns c
ON c.id = t.id
WHERE 1=1
and   t.name = '+@quote+'Vw_'+@quote+'+@CBSID+'+@quote+''+@quote+'
ORDER BY c.colid
;
--PRINT @datacolumns;
'
--PRINT @dsql;
-- following needs params as one string, NOT split.
EXECUTE sp_executesql @dsql, N'@datacolumns varchar(max) OUTPUT, @CBSID varchar(100)', @datacolumns OUTPUT, @CBSID
--PRINT @datacolumns;

SET @sql = 'CREATE OR ALTER VIEW [Vw_'+@CBSID+'_with_colnames] AS'+@crlf+'SELECT '+@crlf+'  '+SUBSTRING(@datacolumns,5,100000)+@crlf+'FROM [Vw_'+@CBSID+']'
--print @sql;
EXEC (@sql);


  --END   DO SOMETHING WITHIN CURSOR
  FETCH NEXT FROM CBSID_Cursor   
    INTO @CBSID;
END   
CLOSE CBSID_Cursor;  
DEALLOCATE CBSID_Cursor;  

