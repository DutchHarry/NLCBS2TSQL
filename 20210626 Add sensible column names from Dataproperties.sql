/*
Add sensible column names from Dataproperties
*/

DECLARE @debug varchar(1) = 'N' --Y,N,I
DECLARE @quote varchar(max) = '''';
DECLARE @crlf varchar(max) = CHAR(13)+CHAR(10);
DECLARE @datacolumns varchar(max)=''
DECLARE @sql nvarchar(max)=''

SELECT @datacolumns = @datacolumns +COALESCE(@crlf+' ,'+
  c.name+' AS ['+
  COALESCE(
    (
      SELECT c.name+' '+p2.[Title] 
      FROM [tblCBS_84721NED_Dataproperties] p1 
      LEFT JOIN [tblCBS_84721NED_Dataproperties] p2
      ON p1.ID = p2.[ID]+2
      WHERE 1=1
      AND 'data_'+p1.[Key] = c.name
      AND ('data_'+p1.[Key] LIKE 'data_Naam_%' OR 'data_'+p1.[Key] LIKE 'data_Omschrijving_%' )
    )
    , 
    (
      SELECT c.name+' '+p2.[Title] 
      FROM [tblCBS_84721NED_Dataproperties] p1 
      LEFT JOIN [tblCBS_84721NED_Dataproperties] p2
      ON p1.ID = p2.[ID]+1
      WHERE 1=1
      AND 'data_'+p1.[Key] = c.name
      AND 'data_'+p1.[Key] LIKE 'data_Code_%'
    )
    , 
    c.name
  )+']'
,'')
FROM sysobjects t
INNER JOIN syscolumns c
ON c.id = t.id
WHERE 1=1
and   t.name = 'Vw_84721NED'
ORDER BY c.colid

SET @sql = 'CREATE OR ALTER VIEW [Vw_84721NED_with_colnames] AS'+@crlf+'SELECT '+@crlf+'  '+SUBSTRING(@datacolumns,5,100000)+@crlf+'FROM [Vw_84721NED]'
print @sql;
EXEC (@sql);
