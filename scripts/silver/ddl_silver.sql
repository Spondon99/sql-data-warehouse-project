/* This is going to be identical to the Bronze layer DDL,
   as we don't change the Data Model in this layer.
   However, we add 1 extra column as metadata (dwh_create_date)
   to each table
*/

IF OBJECT_ID('silver.accounts', 'U') IS NOT NULL
    DROP TABLE silver.accounts;
GO

CREATE TABLE silver.accounts (
	account NVARCHAR(50),
	sector NVARCHAR(50),
	year_established INT,
	revenue DECIMAL(10,2),
	employees INT,
	office_location NVARCHAR(50),
	subsidiary_of NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.products', 'U') IS NOT NULL
    DROP TABLE silver.products;
GO

CREATE TABLE silver.products (
	prod NVARCHAR(50),
	series NVARCHAR(50),
	sales_price INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.sales_pipeline', 'U') IS NOT NULL
    DROP TABLE silver.sales_pipeline;
GO

CREATE TABLE silver.sales_pipeline (
	opportunity_id NVARCHAR(50),
	sales_agent NVARCHAR(50),
	prod NVARCHAR(50),
	account NVARCHAR(50),
	deal_stage NVARCHAR(50),
	engage_date DATETIME,
	close_date DATETIME,
	close_value INT,
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.sales_teams', 'U') IS NOT NULL
    DROP TABLE silver.sales_teams;
GO

CREATE TABLE silver.sales_teams (
	sales_agent NVARCHAR(50),
	manager NVARCHAR(50),
	regional_office NVARCHAR(50),
	dwh_create_date DATETIME2 DEFAULT GETDATE()
);
