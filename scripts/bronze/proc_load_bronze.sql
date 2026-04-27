-- The following script creates a stored procedure to bulk insert the data into appropriate tables
-- Caution: The script uses 'Truncate & Insert' logic, so the previous data will be lost when running the script 

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_bronze_time DATETIME, @end_bronze_time DATETIME;

	BEGIN TRY
		SET @start_bronze_time = GETDATE();

		PRINT '===============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '===============================================';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.accounts';
		TRUNCATE TABLE bronze.accounts;
		PRINT '>> Inserting Data Into: bronze.accounts';
		BULK INSERT bronze.accounts
		FROM 'E:\AI-ML-Data Projects\SQL Data Warehouse Project\SQL Data Warehouse\datasets\accounts.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.products';
		TRUNCATE TABLE bronze.products;
		PRINT '>> Inserting Data Into: bronze.products';
		BULK INSERT bronze.products
		FROM 'E:\AI-ML-Data Projects\SQL Data Warehouse Project\SQL Data Warehouse\datasets\products.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.sales_pipeline';
		TRUNCATE TABLE bronze.sales_pipeline;
		PRINT '>> Inserting Data Into: bronze.sales_pipeline';
		BULK INSERT bronze.sales_pipeline
		FROM 'E:\AI-ML-Data Projects\SQL Data Warehouse Project\SQL Data Warehouse\datasets\sales_pipeline.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.sales_teams';
		TRUNCATE TABLE bronze.sales_teams;
		PRINT '>> Inserting Data Into: bronze.sales_teams';
		BULK INSERT bronze.sales_teams
		FROM 'E:\AI-ML-Data Projects\SQL Data Warehouse Project\SQL Data Warehouse\datasets\sales_teams.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------';
		PRINT '===============================================';
		PRINT '>> Bronze Layer Loaded In: ' + CAST (DATEDIFF(second, @start_bronze_time, @end_bronze_time) AS NVARCHAR) + ' seconds';
		PRINT '===============================================';

	END TRY
	BEGIN CATCH
		PRINT '===============================================';
		PRINT 'Error Occured During Loading Bronze Layer';
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Number:' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State:' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '===============================================';
	END CATCH
END


-- Run the below for executing the procedure
EXEC bronze.load_bronze;
