IF OBJECT_ID('bronze.accounts', 'U') IS NOT NULL
    DROP TABLE bronze.accounts;
GO

CREATE TABLE bronze.accounts (
	account NVARCHAR(50),
	sector NVARCHAR(50),
	year_established INT,
	revenue DECIMAL(10,2),
	employees INT,
	office_location NVARCHAR(50),
	subsidiary_of NVARCHAR(50)
);
GO

IF OBJECT_ID('bronze.products', 'U') IS NOT NULL
    DROP TABLE bronze.products;
GO

CREATE TABLE bronze.products (
	prod NVARCHAR(50),
	series NVARCHAR(50),
	sales_price INT
);
GO

IF OBJECT_ID('bronze.sales_pipeline', 'U') IS NOT NULL
    DROP TABLE bronze.sales_pipeline;
GO

CREATE TABLE bronze.sales_pipeline (
	opportunity_id NVARCHAR(50),
	sales_agent NVARCHAR(50),
	prod NVARCHAR(50),
	account NVARCHAR(50),
	deal_stage NVARCHAR(50),
	engage_date DATETIME,
	close_date DATETIME,
	close_value INT
);
GO

IF OBJECT_ID('bronze.sales_teams', 'U') IS NOT NULL
    DROP TABLE bronze.sales_teams;
GO

CREATE TABLE bronze.sales_teams (
	sales_agent NVARCHAR(50),
	manager NVARCHAR(50),
	regional_office NVARCHAR(50)
);
