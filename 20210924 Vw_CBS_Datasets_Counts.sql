USE master;
GO

/*
download the full dataset of CBS catalogs and check which catalogs need updating
*/

/*
I installed R 4.1.1 as an external language in SQL2019 (maybe you can adapt for the R-services included with SQL)

Following needs installation of R as an external language and naming it 'HR'
see:
https://docs.microsoft.com/en-us/sql/machine-learning/install/custom-runtime-r
extension fo R can be found here:
https://github.com/microsoft/sql-server-language-extensions/releases/tag/R-v1.1.0

For completeness, Python is slightly different
https://nielsberglund.com/2021/01/09/solve-python-issues-in-sql-server-machine-learning-services-after-deploying-python-3.9/
with a working (for v3.9) extension here:
https://nielsberglund.com/downloads/fixpython/python-lang-extension-windows-3.9.zip

You'd need to give the Launchpad and 'ALL APPLICATION PACKAGES'
C:/testFolder
below the same rights as your R-directory
And of course unblcok the AppContainer(s) in Firewall

And you need to make sure all dependencies are installed in the ib that Launchpad sees!

Install R with admin rights
Start C:\Program Files\R\R-4.1.1\bin\x64\R.exe with admin rights
install.packages("Rcpp", dependencies = TRUE, lib="C:\\Program Files\\R\\R-4.1.1\\library");
pick a repo and install

Add 'C:\Program Files\R\R-4.1.1\bin\x64' (wh quotes) to PATH
Create System variable R_HOME 'C:\Program Files\R\R-4.1.1' (wh quotes)

icacls "C:\Program Files\R\R-4.1.1" /grant "NT Service\MSSQLLAUNCHPAD":(OI)(CI)RX /T
icacls "C:\Program Files\R\R-4.1.1" /grant "ALL APPLICATION PACKAGES":(OI)(CI)RX /T
icacls "C:\testFolder" /grant "ALL APPLICATION PACKAGES":(OI)(CI)RX /T

install.packages("remotes", dependencies = TRUE, lib="C:\\Program Files\\R\\R-4.1.1\\library");
--get cbsodataR from github
remotes::install_github("edwindj/cbsodataR",force=TRUE,lib="C:\\Program Files\\R\\R-4.1.1\\library")

--cbsodataR needs jsonlite and whisker installed in the correct lib path
install.packages('jsonlite', dependencies = TRUE, lib="C:\\Program Files\\R\\R-4.1.1\\library")
install.packages('whisker', dependencies = TRUE, lib="C:\\Program Files\\R\\R-4.1.1\\library")

--DROP EXTERNAL LANGUAGE [HR];
CREATE EXTERNAL LANGUAGE [HR]
FROM (
  CONTENT = N'C:\temp\110R-lang-extension-windows-release.zip'
, FILE_NAME = 'libRExtension.dll'
);
GO

--testing if it works:
EXEC sp_execute_external_script
	@language =N'HR',
	@script=N'
print(R.home());
print(file.path(R.home("bin"), "R"));
print(R.version);
print("Hello RExtension!");'


*/

EXECUTE sp_execute_external_script 
@language = N'HR',
@script = N'
library(cbsodataR)
setwd("C://testFolder")
write.csv(cbs_get_datasets(catalog=NULL),"datasets.csv")
'
USE CBS;
GO

DROP TABLE IF EXISTS Tbl_CBS_Datasets;
GO

CREATE TABLE Tbl_CBS_Datasets(
  [CBSLineNumber] INT NULL --varchar(8000) NULL
, [Updated] DATETIME NULL --varchar(8000) NULL
, [Identifier] varchar(8000) NULL
, [Title] varchar(8000) NULL
, [ShortTitle] varchar(8000) NULL
, [ShortDescription] varchar(MAX) NULL
, [Summary] varchar(8000) NULL
, [Modified] DATETIME NULL --varchar(8000) NULL
, [MetaDataModified] DATETIME NULL --varchar(8000) NULL
, [ReasonDelivery] varchar(8000) NULL
, [ExplanatoryText] varchar(8000) NULL
, [OutputStatus] varchar(8000) NULL
, [Source] varchar(8000) NULL
, [Language] varchar(8000) NULL
, [Catalog] varchar(8000) NULL
, [Frequency] varchar(8000) NULL
, [Period] varchar(8000) NULL
, [SummaryAndLinks] varchar(8000) NULL
, [ApiUrl] varchar(8000) NULL
, [FeedUrl] varchar(8000) NULL
, [DefaultPresentation] varchar(8000) NULL
, [DefaultSelection] varchar(8000) NULL
, [GraphTypes] varchar(8000) NULL
, [RecordCount] BIGINT NULL --varchar(8000) NULL
, [ColumnCount] INT NULL --varchar(8000) NULL
, [SearchPriority] TINYINT NULL --varchar(8000) NULL
)
;


BULK INSERT [Tbl_CBS_Datasets]
FROM 'C:\testFolder\datasets.csv'
WITH (
  CODEPAGE = 'RAW'
, DATAFILETYPE  = 'char'
, FIRSTROW = 2
, FORMAT = 'CSV'
, FIELDQUOTE = '"'
, FIELDTERMINATOR  = ','
, ROWTERMINATOR ='\n'
, TABLOCK 
);
GO


CREATE OR ALTER VIEW Vw_CBS_Datasets_Counts AS
WITH cte0 AS (
SELECT 
  sc.name +'.'+ ta.name AS [TableName]
, SUBSTRING(ta.name,8,CHARINDEX('_',ta.name,8)-8) AS [CBSID]
, SUM(pa.rows) RowCnt
FROM 
    sys.tables ta
INNER JOIN sys.partitions pa
    ON pa.OBJECT_ID = ta.OBJECT_ID
INNER JOIN sys.schemas sc
    ON ta.schema_id = sc.schema_id
WHERE 1=1
AND ta.is_ms_shipped = 0 
AND pa.index_id IN (1,0)
AND ta.name LIKE 'TblCBS_%data'
GROUP BY 
  sc.name
, ta.name
--ORDER BY 
--  SUM(pa.rows) DESC
), cte1 AS (
SELECT 
  d.[CBSLineNumber]
, d.[Updated]
, d.[Identifier]  AS [CBSID]
--, d.[Title]
, d.[ShortTitle]
--, d.[ShortDescription]
--, d.[Summary]
--, d.[Modified]
--, d.[MetaDataModified]
--, d.[ReasonDelivery]
--, d.[ExplanatoryText]
--, d.[OutputStatus]
--, d.[Source]
--, d.[Language]
, d.[Catalog]
--, d.[Frequency]
, d.[Period]
--, d.[SummaryAndLinks]
--, d.[ApiUrl]
--, d.[FeedUrl]
--, d.[DefaultPresentation]
--, d.[DefaultSelection]
--, d.[GraphTypes]
, d.[RecordCount]
, d.[ColumnCount]
--, d.[SearchPriority]
FROM [CBS].[dbo].[Tbl_CBS_Datasets] d
WHERE 1=1
), cte2 AS (
SELECT 
  [Schema] = s.name
, [Table] = t.name
, SUBSTRING(t.name,8,CHARINDEX('_',t.name,8)-8) AS [CBSID]
, [ColCnt] = COUNT(*)
FROM sys.columns c
INNER JOIN sys.tables t 
ON c.object_id = t.object_id
INNER JOIN sys.schemas s 
ON t.schema_id = s.schema_id
WHERE 1=1
AND t.name LIKE 'TblCBS_%data'
GROUP BY 
  t.name
, s.name
)
SELECT TOP 1000000000
  cte1.*
, cte0.RowCnt
, cte2.ColCnt
, cte1.RecordCount * cte1.ColumnCount AS CBSCellCnt
, cte1.RecordCount - cte0.RowCnt AS [RowCntDiff]
, cte1.ColumnCount - cte2.ColCnt + 1 AS [ColCntDiff] --data tables have additional column ID referring to data line number in CSV file
, 'cbs_download_table(id="'+cte1.CBSID+'",catalog="'+cte1.[Catalog]+'")' AS [R_Download_string]
FROM cte1 
LEFT JOIN cte0
ON cte1.[CBSID]=cte0.[CBSID]
LEFT JOIN cte2
ON cte1.[CBSID]=cte2.[CBSID]
ORDER BY 
  cte1.CBSLineNumber
;
GO

SELECT
*
FROM Vw_CBS_Datasets_Counts
WHERE 1=1
AND [RowCnt] IS NOT NULL
AND RowCntDiff<>0
--AND [Catalog] <>'CBS'
ORDER BY CBSCellCnt DESC

/*
--after this you can re-download the catalogs that have been updated
USE master;  --that's the only DB where I've installed the language extensions for now
GO

--e.g.
EXECUTE sp_execute_external_script 
@language = N'HR',
@script = N'
library(cbsodataR)
setwd("C://testFolder")
cbs_download_table(id="37610",catalog="CBS")
';
GO

*/

