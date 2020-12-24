/*
ai_IntegrityCheck.sql - a script.
    
#  Data warehouse referential integrity checker

Aimed at data warehouses where dimension tables are prefixed 'dim', fact tables 'fact'
and where the dimension primary key column name is part or all of the fact table foreign key column name.
And there are no foreign key constraints:(

## How it works

  @Template holds a dynamic sql template for counting number of rows in fact table, the number of 
  orphaned foreign keys in the fact table, and the number of distinct orphaned values, etc.  It 
  contains placeholders ('{DimensionTable}', '{PrimaryKey}', '{FactTable}', '{ForeignKey}')  
  which are all that are requried to make it run a test on a pair of tables.
  
  The CTE named "Dimensions" is a list of tables beginning "dim" together with the name of the 
  primary key column.
  
  The CTE "FactColumns" is all tables beginning "fact" that have a column name that contains the
  primary key column name.  ** These are identified by their names rather than by foreign key constraint.
  These two tables are joined to produce a set of pairs of dimension and fact tables that is
  used to populate a cursor.
  
  The cursor loop substitutes the four placeholders in the sql template, and the resulting 
  sql statement is executed.
  
# OUTPUT
        DimensionTable    :    the dimension table e.g. [dbo].[dimCustomer]
        PrimaryKey        :   the primary key of the dimension table  e.g. [CustomerKey]
        FactTable        :    a fact table that has a foreign key to a dimension e.g. [dbo].[factSales]
        ForeignKey        :    the foreign key of the fact table e.g. [CustomerKey] or [DeliveryCustomerKey]
        FactRows        :   total number of rows in the fact table
        NbrOfOrphans    :    number of rows in fact table where the foreign key does not match the dimension primary key
        NbrOfOrphanedValues : number of distinct values that do not match a dimension primary key value
        MaxOrphanedValue    : the highest orphaned foreign key value (including Null).  'n/a' if there are no orphaned rows.
        NbrOfSpecialRows    : number of rows in fact table where foreign key is < 0. "Unknown" and suchlike.

# Change log
        18-08-2020 AI version 1.
*/



CREATE TABLE
    #ReferentialIntegrityTests(
        RunTime                datetime NOT NULL,
        DimensionTable            nvarchar(300) NOT NULL,
        PrimaryKey                nvarchar(300) NOT NULL,
        FactTable                nvarchar(300) NOT NULL,
        ForeignKey                nvarchar(300) NOT NULL,
        FactRows                int,
        NbrOfOrphans            int,
        NbrOfOrphanedValues        int,
        MaxOrphanedValue        varchar(20),
        NbrOfSpecialRows        int
    )


DECLARE @sql nvarchar(max),
        @Template nvarchar(max),
        @DimensionTable nvarchar(300),
        @FactTable nvarchar(300),
        @PrimaryKey nvarchar(300),
        @ForeignKey nvarchar(300),
        @cur CURSOR;


SET @Template = N';WITH RI AS
(
    SELECT 
        d.{PrimaryKey} AS PrimaryKey,
        f.{ForeignKey} AS ForeignKey,
        Count(*) AS NbrOfRows
    FROM 
        {FactTable} f
        LEFT JOIN {DimensionTable} d ON f.{ForeignKey} = d.{PrimaryKey} 
    GROUP BY
        d.{PrimaryKey},
        f.{ForeignKey}
)
INSERT #ReferentialIntegrityTests (
    RunTime                ,
    DimensionTable        ,
    PrimaryKey            ,
    FactTable            ,
    ForeignKey            ,
    FactRows            ,
    NbrOfOrphans        ,
    NbrOfOrphanedValues    ,
    MaxOrphanedValue    ,
    NbrOfSpecialRows
)
SELECT 
    RunTime = GETDATE(),
    DimensionTable    = ''{DimensionTable}'',
    PrimaryKey        = ''{PrimaryKey}'',
    FactTable        = ''{FactTable}'',
    ForeignKey        = ''{ForeignKey}'',
    FactRows            = ISNULL(( SELECT SUM(NbrOfRows) FROM RI ), 0),
    NbrOfOrphans        = ISNULL(( SELECT SUM(NbrOfRows) FROM RI WHERE PrimaryKey IS NULL), 0),
    NbrOfOrphanedValues = ISNULL(( SELECT COUNT(*) FROM RI WHERE PrimaryKey IS NULL GROUP BY PrimaryKey), 0),
    MaxOrphanedValue    = ISNULL((SELECT MAX( ISNULL( CAST(ForeignKey AS varchar(25)), ''Null'')) FROM RI WHERE PrimaryKey IS NULL), ''n/a''),
    NbrOfSpecialRows    = ISNULL( (SELECT SUM(NbrOfRows) FROM RI WHERE ForeignKey < 0), 0)';
    
SET @cur = CURSOR STATIC FOR
WITH Dimensions AS
(
    SELECT 
         QUOTENAME(S.NAME) + '.' + QUOTENAME(t.[name]) AS TableName
        ,QUOTENAME(c.name) AS PrimaryKey

    FROM sys.objects t
        INNER JOIN sys.schemas S ON S.schema_id =t.schema_id
        INNER JOIN sys.columns c ON c.object_id = t.object_id
        INNER JOIN sys.index_columns  ic ON ic.object_id = t.object_id
                AND ic.column_id = c.column_id
        INNER JOIN sys.key_constraints kc ON kc.parent_object_id = ic.object_id
                AND ic.index_id = kc.unique_index_id
    WHERE 
        t.[type] = 'U'
        AND kc.[type] = 'PK'
        AND t.[name] LIKE 'dim%'

), 
FactColumns AS
(
    SELECT 
        QUOTENAME(S.NAME) + '.' + QUOTENAME(t.[name]) AS TableName
        ,QUOTENAME(c.name) AS ColumnName

    FROM sys.objects t
        INNER JOIN sys.schemas S ON S.schema_id =t.schema_id
        INNER JOIN sys.columns c ON c.object_id = t.object_id
    WHERE 
        t.[type] = 'U'
        AND t.[name] LIKE 'fact%'
)

    SELECT
        d.TableName AS DimensionTable,
        d.PrimaryKey AS PrimaryKey,
        f.TableName AS FactTable,
        f.ColumnName As ForeignKey
    FROM
        Dimensions d
        CROSS JOIN FactColumns f 
    WHERE
        CHARINDEX(REPLACE(d.PrimaryKey, '[', ''), f.ColumnName) > 0;
OPEN @cur

WHILE 1 = 1
BEGIN
    FETCH @cur INTO @DimensionTable,
                    @PrimaryKey,
                    @FactTable,
                    @ForeignKey
    IF @@fetch_status <> 0 BREAK
    SET @sql = REPLACE(@Template, '{DimensionTable}', @DimensionTable)
    SET @sql = REPLACE(@sql, '{FactTable}', @FactTable)
    SET @sql = REPLACE(@sql, '{PrimaryKey}', @PrimaryKey)
    SET @sql = REPLACE(@sql, '{ForeignKey}', @ForeignKey)

    EXEC sp_executesql @sql
END
        
SELECT * FROM #ReferentialIntegrityTests ORDER BY NbrOfOrphans desc, FactRows desc
DROP TABLE #ReferentialIntegrityTests
