/*
	Loads Bronze->Silver
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @start_silver_time DATETIME, @end_silver_time DATETIME; 
    BEGIN TRY
        SET @start_silver_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';


		-- Loading silver.accounts
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.accounts';
		TRUNCATE TABLE silver.accounts;
		PRINT '>> Inserting Data Into: silver.accounts';
		INSERT INTO silver.accounts (
			account,
			sector, 
			year_established, 
			revenue, 
			employees, 
			office_location,
			subsidiary_of
		)
		SELECT
			TRIM(account) AS account,
			TRIM(sector) AS sector,
			year_established,
			ISNULL(revenue, 0) AS revenue,
			ISNULL(employees, 0) AS employees,
			TRIM(office_location) AS office_location,
			TRIM(subsidiary_of) AS subsidiary_of
		FROM 
			bronze.accounts;
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.products
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.products';
		TRUNCATE TABLE silver.products;
		PRINT '>> Inserting Data Into: silver.products';
		INSERT INTO silver.products (
			prod,
			series,
			sales_price
		)
		SELECT
			TRIM(prod) AS prod,
			TRIM(series) AS series,
			ISNULL(sales_price, 0) AS sales_price
		FROM bronze.products;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
        -- Loading silver.sales_pipeline
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.sales_pipeline';
		TRUNCATE TABLE silver.sales_pipeline;
		PRINT '>> Inserting Data Into: silver.sales_pipeline';
		INSERT INTO silver.sales_pipeline (
			opportunity_id,
			sales_agent,
			prod,
			account,
			deal_stage,
			engage_date,
			close_date,
			close_value
		)
		SELECT 
			opportunity_id,
			TRIM(sales_agent) AS sales_agent,
			TRIM(prod) AS prod,
			TRIM(account) AS account,
			TRIM(deal_stage) AS deal_stage,
			engage_date,
			close_date,
			ISNULL(close_value, 0) AS close_value
		FROM bronze.sales_pipeline;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
        -- Loading silver.sales_teams
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.sales_teams';
		TRUNCATE TABLE silver.sales_teams;
		PRINT '>> Inserting Data Into: silver.sales_teams';
		INSERT INTO silver.sales_teams (
			sales_agent,
			manager,
			regional_office
		)
		SELECT
			TRIM(sales_agent) AS sales_agent,
			TRIM(manager) AS manager,
			TRIM(regional_office) as regional_office
		FROM bronze.sales_teams;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @end_silver_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_silver_time, @end_silver_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
