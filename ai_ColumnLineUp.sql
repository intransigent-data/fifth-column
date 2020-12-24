IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'dbo.ai_ColumnLineUp')
	EXEC ('CREATE PROC dbo.ai_ColumnLineUp AS SELECT 1 AS DummyColumn')
GO

CREATE PROCEDURE dbo.ai_ColumnLineUp
/*
# How consistent are the column definitions in this database?

List all the column names used in the current database together with their
types.  If all columns with the same name have the same data type all is good.
If there are variances these will appear in adjacent rows in the list.

# Parameters

* @ShowAllCols (0) : 0 = only show inconsistent columns. 1 = show all columns.
* @ExcludeSchema   : name of schema to exclude from checks (e.g. 'raw' staging schemas).

# Output

* Column:  column name
* Type:  the condensed data type expression
* Uses_count: the number of instances of a column with the name in this database
* Tables: comma separated list of tables that the column name appears in.

# History
* 1/6/2020 AI Created.
*/
	@ShowAllColumns bit = 0,
	@ExcludeSchema sysname = N''


AS
	DECLARE @max_len_tables_list int = 1000

	;WITH cols AS (
		SELECT
		[Number]    = f.column_ordinal,
		[Column]      = f.name,
		[Type]      = f.system_type_name,
		[Nullable]  = f.is_nullable,
		SourceTable = QUOTENAME(s.name) + N'.' + QUOTENAME(t.name)
		FROM 
		sys.tables AS t
		INNER JOIN sys.schemas AS s ON t.[schema_id] = s.[schema_id]
		CROSS APPLY sys.dm_exec_describe_first_result_set
			(
				N'SELECT * FROM ' + DB_NAME() + N'.' + QUOTENAME(s.name) + N'.' + QUOTENAME(t.name),
				N'', 0
			) AS f
		WHERE s.name <> @ExcludeSchema
	),
	cols2 AS (
		SELECT
			[Uses_count]	= Count(*),
			[Column]		= [Column],
			[Type]			= [Type],
			[Tables]		= LEFT( STRING_AGG( CAST(SourceTable AS nvarchar(max)), ', '), @max_len_tables_list),
			Lag_column		= LAG([Column]) OVER (ORDER BY [Column]),
			Lead_column		= LEAD([Column]) OVER (ORDER BY [Column])
		FROM
			cols
		GROUP BY
			[Column],
			[Type]
	)
	SELECT
		[Column],
		[Type],
		[Uses_count],
		[Tables] = CASE WHEN LEN([Tables]) > @max_len_tables_list THEN [Tables] + N'...' ELSE [Tables] END
	FROM
		cols2
	WHERE
		(
			([Column] = Lag_column OR [Column] = Lead_column)
			OR @ShowAllColumns = 1
		)
	ORDER BY [Column], [Type]
