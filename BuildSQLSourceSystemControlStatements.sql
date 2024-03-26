CREATE VIEW [utl].[BuildSQLSourceSystemControlStatements] AS 

/******************************************************************************************************
 Author: David Dimmen & David Corrigall & Ben Fulton
 Date: 2023-07-21
 Purpose: Builds create statements, select statements, and psa procs dynamically
 
 Notes:
 Change History:
	2023.08.07 - David Corrigall
		Adding logic to highlight active tables as any table that has a row count greater than 0
		Could change the join to partition to be an inner join as opposed to a left join in order to filter our records, but trying to evaluate if there would ever be a need
		for a table with 0 rows to be staged. Most likely not. Will review with data team**

		Adding notes to filters on the CTEs for more context around indexes and partitions

	2023.08.10 L. Ochs - Removed brackets on primary key columns. Spark SQL doesn't like brackets. 
	2023.10.09 L. Ochs - Added in logic to exclude a specific column. Primarily for PII
	2024.03.14 B.Fulton - Added table inclusion list for exceedingly large databases 
	2024.03.21 B.Fulton - Added column aliasing to remove spaces.
******************************************************************************************************/
WITH [Objects] AS (
    SELECT *
	FROM [dbo].[SQLSource_sys_objects] o
	WHERE o.[type_desc] = 'USER_TABLE'
    )

, [Schemas] AS (
    SELECT * 
    FROM [dbo].[SQLSource_sys_schemas] AS s
    )

, [Tables] AS (
    SELECT * 
    FROM [dbo].[SQLSource_sys_tables] AS t
    )

, [Columns] AS (
    SELECT * 
    FROM [dbo].[SQLSource_sys_columns] AS c
) 

, [IS_Columns] AS (
    SELECT * 
    FROM [dbo].[SQLSource_InfoSchema_Columns] AS ISC
) 

, [Partitions] AS ( -- The granularity of this table needs to be reduced to match the [Objects] granularity
    SELECT [object_id]
		 ,[p].[database_id] 
		 ,[p].[ServerName]  
		 ,[p].[ClientName]
		 ,SUM([rows]) as [rows]  
    FROM [dbo].[SQLSource_sys_partitions] AS p
	WHERE p.index_id < 2 -- Anything greater than 2 is a non-clustered index 
	-- Since there can be only one way in which records are physically stored in a database table, 
		--there can be only one clustered index per table. By default a clustered index is created on a primary key column
    GROUP BY [object_id],[p].[database_id],[p].[ServerName],[p].[ClientName]
	HAVING SUM([rows]) <> 0
)

, [Indexes] AS ( --Primary Keys and Unique Constraints
	SELECT * 
		, ROW_NUMBER() OVER (
				PARTITION BY [i].[object_id], i.DatabaseName, i.ServerName, i.ClientName 
					ORDER BY CASE 
								WHEN [i].[is_primary_key] = 1		THEN 1
								WHEN [i].[is_unique_constraint] = 1	THEN 2
								WHEN [i].[is_unique] = 1			THEN 3
								ELSE 4
							  END, [i].[index_id] ASC) as RN 
    FROM [dbo].[SQLSource_sys_indexes] AS i
	WHERE [i].[type] <> 0 -- type of 0 refers to Heap which we do not need to include
	  AND [i].[is_unique] = 1
) 

, [IndexColumns] AS (
    SELECT * 
    FROM [dbo].[SQLSource_sys_index_columns] AS ic
)

, [Databases] AS (  
    SELECT a.ClientName, a.ServerName, a.DatabaseName, a.SourceAbbreviation, a.SourceSystemName, a.DataLakeName, a.SourceDataStructure 
	,b.database_id
    FROM [etl].[ControlTable_SourceDB] a
	LEFT JOIN [dbo].[SQLSource_sys_objects] b 
		ON [a].[ServerName]	= [b].[ServerName]  
		AND [a].[ClientName]	= [b].[ClientName] 
		AND [a].[DatabaseName]	= [b].[DatabaseName]
	GROUP BY a.ClientName, a.ServerName, database_id, a.DatabaseName, SourceAbbreviation, a.SourceSystemName, a.DataLakeName, a.SourceDataStructure
)

, [DeltaColumns] AS (
	SELECT
		*
	FROM [utl].[SQLSource_DeltaColumns]
)

, [TableExclusion] AS (
	SELECT
		*
	FROM [utl].[SQLSource_TableExclusion]

), [TableInclusionClientList] AS (
	SELECT DISTINCT
		[ClientName],
		[SourceName]
	FROM [utl].[SQLSource_TableInclusion]
)

, [TotalList] AS (
    SELECT 
		[d].[ClientName]												AS [ClientName]
		, [d].[DataLakeName]											AS [DataLakeName]
		, [d].[SourceDataStructure]										AS [SourceDataStructure]
		, [d].[SourceSystemName]										AS [SourceSystemName]
        , [d].[DatabaseName]											AS [SourceDatabaseName]
		, [d].[ServerName]												AS [SourceServerName]
		, [o].[object_id]												AS [ObjectId]
		, [s].[name]													AS [SourceSchemaName]
		,'['+[s].[name]+'].['+[t].[name]+']'							AS [SchemaTable]
		, [t].[name]													AS [SourceTableName]
		, [c].[name]													AS [ColumnName]
		, '['+[c].[name]+']'											AS [FormattedColumnName]        
		, [c].[column_id]												AS [ColumnId]
		, [o].[type_desc]												AS [TypeDesc]
		, [icl].[ORDINAL_POSITION]										AS [OrdinalPosition]
		, CONCAT([s].[name],'_',[t].[name],'.parquet')					AS [StagingDataFileName]
		, CONCAT([s].[name],'_',[t].[name],'_PK.parquet')				AS [PrimaryKeyFileName]
		, CASE
			WHEN [ic].[column_id] IS NULL THEN NULL
			ELSE CONCAT(
				  '[', CASE 
					WHEN [ic].[column_id] IS NOT NULL THEN [c].[name] 
					ELSE NULL
				  END, ']')
		  END															AS [FormattedPKColumn]

		, CASE
			WHEN [ic].[column_id] IS NULL THEN NULL
			ELSE CONCAT(
				  't.', CASE 
					WHEN [ic].[column_id] IS NOT NULL THEN [c].[name]
					ELSE NULL
				  END, ' = s.', CASE 
					WHEN [ic].[column_id] IS NOT NULL THEN [c].[name]
					ELSE NULL
				  END)
		  END															AS [PKClauseColumns]
		, CASE WHEN COUNT(ic.column_id) OVER (
							PARTITION BY [t].[ClientName], [t].[ServerName], [t].[DatabaseName], [t].[name]  
								/*ORDER BY [t].[name] ROWS UNBOUNDED PRECEDING */) = 0 THEN 1
			ELSE 0
		  END															AS [HasNoPK]
		, [ic].[column_id]												AS [PKColumnId]
		, [i].[name]													AS [IndexName]
		, [i].[index_id]												AS [IndexId]
		, [p].[rows]													AS [SourceRowCount]
		, COALESCE(cdc.DeltaColumn, tdc.DeltaColumn, gdc.DeltaColumn)	AS [DeltaColumn]
		, CASE 
			WHEN COALESCE(gte.TableNameForExclusion, cte.TableNameForExclusion) IS NOT NULL 
				  OR 
				          ([cti].TableNameForInclusion IS NULL AND ticl.[SourceName] IS NOT NULL )THEN 1 
			ELSE 0 
		  END															AS [ExcludeTable]
		, CASE 
			WHEN COALESCE(gce.ColumnNameForExclusion, cce.ColumnNameForExclusion) IS NOT NULL THEN 1 
			ELSE 0 
		  END															AS [ExcludeColumn]
    FROM [Objects] [o] 
    LEFT JOIN [Databases] [d]
        ON  [o].[database_id] = [d].[database_id]
		AND [o].[ServerName]  = [d].[ServerName]
		AND [o].[ClientName]  = [d].[ClientName] 
    LEFT JOIN [Schemas] [s]
        ON  [o].[schema_id]   = [s].[schema_id] 
		AND [o].[database_id] = [s].[database_id]
		AND [o].[ServerName]  = [s].[ServerName]
		AND [o].[ClientName]  = [s].[ClientName] 
    LEFT JOIN [Columns] [c] 
        ON  [o].[object_id]   = [c].[object_id] 
		AND [o].[database_id] = [c].[database_id]
		AND [o].[ServerName]  = [c].[ServerName]
		AND [o].[ClientName]  = [c].[ClientName] 
    LEFT JOIN [Tables] [t] 
        ON [o].[object_id]    = [t].[object_id]
		AND [o].[database_id] = [t].[database_id]
		AND [o].[ServerName]  = [t].[ServerName]
		AND [o].[ClientName]  = [t].[ClientName]
    LEFT JOIN [Partitions] [p]
        ON  [o].[object_id]   = [p].[object_id]
		AND [o].[database_id] = [p].[database_id]
		AND [o].[ServerName]  = [p].[ServerName]
		AND [o].[ClientName]  = [p].[ClientName]
	LEFT JOIN [Indexes] [i]
		ON  [o].[object_id]   = [i].[object_id]
	   	AND [o].[database_id] = [i].[database_id]
		AND [o].[ServerName]  = [i].[ServerName]
		AND [o].[ClientName]  = [i].[ClientName]
		AND [i].[RN] = 1
    LEFT JOIN [IndexColumns] [ic] 
        ON  [i].[object_id]   = [ic].[object_id]
		AND [i].[index_id]    = [ic].[index_id]
		AND [c].[column_id]   = [ic].[column_id]
	   	AND [i].[database_id] = [ic].[database_id]
		AND [i].[ServerName]  = [ic].[ServerName]
		AND [i].[ClientName]  = [ic].[ClientName]
	LEFT JOIN [IS_Columns] icl
	    ON [s].[name]			= [icl].[TABLE_SCHEMA]
	    AND [t].[name]			= [icl].[TABLE_NAME]
	    AND [c].[name]			= [icl].[COLUMN_NAME]
		AND [d].[database_id]	= [icl].[database_id]
		AND [d].[ServerName]	= [icl].[ServerName]
		AND [d].[ClientName]	= [icl].[ClientName]
	LEFT JOIN [DeltaColumns] gdc --General Delta Columns by Source System. Each table has the same delta column
		ON d.SourceSystemName	= gdc.SourceName
		AND TRIM([c].[name])	= gdc.DeltaColumn
		AND gdc.TableName IS NULL
		AND gdc.ClientName IS NULL
	LEFT JOIN [DeltaColumns] tdc --General Delta Columns by Source System. Tables have differing delta columns
		ON d.SourceSystemName	= tdc.SourceName
		AND TRIM([c].[name])	= tdc.DeltaColumn
		AND t.[name]			= tdc.TableName
		AND tdc.ClientName IS NULL
	LEFT JOIN [DeltaColumns] cdc --Client specific delta columns
		ON d.SourceSystemName	= cdc.SourceName
		AND TRIM([c].[name])	= cdc.DeltaColumn
		AND t.[name]			= cdc.TableName
		AND	o.ClientName		= cdc.ClientName
		AND o.DatabaseName		= cdc.DatabaseName
		AND o.ServerName		= cdc.ServerName
	LEFT JOIN [TableExclusion] gte --General tables to exclude by Source System. (applies to all clients using the system)
		ON d.SourceSystemName = gte.SourceName
		AND t.[name] = gte.TableNameForExclusion
		AND gte.ClientName IS NULL
		AND gte.ColumnNameForExclusion IS NULL
	LEFT JOIN [TableExclusion] cte --Client specific tables to exclude by Source System. 
		ON d.SourceSystemName	= cte.SourceName
		AND t.[name]			= cte.TableNameForExclusion
		AND	o.ClientName		= cte.ClientName
		AND cte.ColumnNameForExclusion IS NULL
	LEFT JOIN [TableExclusion] gce --General columns to exclude by Source System. (applies to all clients using the system)
		ON d.SourceSystemName	= gce.SourceName
		AND t.[name]			= gce.TableNameForExclusion
		AND TRIM([c].[name])	= gce.ColumnNameForExclusion
		AND gce.ClientName IS NULL
	LEFT JOIN [TableExclusion] cce --Client specific columns to exclude by Source System. 
		ON d.SourceSystemName	= cce.SourceName
		AND t.[name]			= cce.TableNameForExclusion
		AND TRIM([c].[name])	= cce.ColumnNameForExclusion
		AND	o.ClientName		= cce.ClientName
	LEFT JOIN [utl].[SQLSource_TableInclusion] [cti] -- Client specific tables to include per source
		ON d.SourceSystemName = cti.[SourceName]
		AND	o.ClientName	  = cti.[ClientName]
		AND t.name			  = cti.TableNameForInclusion
		AND s.schema_id 	  = cti.SchemaId		 -- Includes schema as tables can be shared across "company" schemas
	LEFT JOIN [TableInclusionClientList] [ticl]
		ON  o.ClientName		= ticl.ClientName
		AND d.SourceSystemName	= ticl.SourceName


) 

SELECT
	[ClientName]														AS [ClientName]
	,[DataLakeName]														AS [DataLakeName]
	,[SourceDataStructure]												AS [SourceDataStructure]
	,[SourceSystemName]													AS [SourceSystemName]
	,[SourceServerName]													AS [SourceServerName]
	,[SourceDatabaseName]												AS [SourceDatabaseName]
	,[SourceSchemaName]													AS [SourceSchemaName]
	,[SourceTableName]													AS [SourceTableName]
	,ISNULL([SourceRowCount],0)											AS [SourceRowCount]
	,'SELECT 
		' + STRING_AGG(CONVERT(VARCHAR(MAX),CONCAT([FormattedColumnName], ' AS ',REPLACE([FormattedColumnName],' ','_') ) ), '
		, ') WITHIN GROUP (ORDER BY [OrdinalPosition] ASC) + ' 
		, '''+[SourceServerName]+''' AS [SourceServerName] 
		, '''+[SourceDatabaseName]+''' AS [SourceDatabaseName] 
	 FROM ['+[SourceDatabaseName]+'].'+[SchemaTable] + ' WITH (NOLOCK)'	AS [SourceSelectStatement]

	,'SELECT 
		' + STRING_AGG(
        CONVERT(VARCHAR(MAX),
            CASE 
                WHEN [FormattedPKColumn] IS NOT NULL 
                THEN CONCAT([FormattedPKColumn], ' AS ', REPLACE([FormattedPKColumn], ' ', '_'))
                ELSE NULL
            END
        ), '
        , ') WITHIN GROUP (ORDER BY [OrdinalPosition] ASC) + ' 
		, '''+[SourceServerName]+''' AS [SourceServerName] 
		, '''+[SourceDatabaseName]+''' AS [SourceDatabaseName] 
	 FROM ['+[SourceDatabaseName]+'].'+[SchemaTable] + ' WITH (NOLOCK)'	AS [SourceSelectPKStatement]
	
	,CASE WHEN HasNoPK = 0 THEN
		CONCAT('
		',STRING_AGG(CONVERT(VARCHAR(MAX), [PKClauseColumns]), '
			AND ') WITHIN GROUP (ORDER BY [OrdinalPosition] ASC), '
			')
		ELSE NULL 
	 END																AS [PrimaryKeyMergeStatement]
	,[StagingDataFileName]												AS [StagingDataFileName]
	,CASE WHEN HasNoPK = 0 THEN [PrimaryKeyFileName] ELSE NULL END		AS [PrimaryKeyFileName]
	,CASE WHEN [ExcludeTable] = 1 OR ISNULL([SourceRowCount],0) = 0	THEN 0
		  ELSE 1
	 END																AS [IsTableConsideredActive]
	,CASE
		WHEN MAX([DeltaColumn]) IS NULL
			THEN 0
		ELSE 1
	END																	AS [IsDelta]
	,CASE
		WHEN MAX([DeltaColumn]) IS NULL
			THEN NULL
		ELSE MAX([DeltaColumn])
	END																	AS [DeltaColumn]
	,NULL																AS [DeltaValue]
	,100																AS [SequenceNumber]
FROM [TotalList]
WHERE [ExcludeColumn] = 0
GROUP BY 
	[ClientName]
	,[DataLakeName]
	,[SourceDataStructure]
	,[SourceSystemName]
	,[SourceServerName]
	,[SourceDatabaseName]
	,[SourceSchemaName]
	,[SourceTableName]
	,[SourceRowCount]
	,[ExcludeTable]
	,[StagingDataFileName]
	,[PrimaryKeyFileName]
	,[HasNoPK]
	,[SchemaTable]

GO


