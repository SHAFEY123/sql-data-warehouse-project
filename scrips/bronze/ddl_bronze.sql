/*
===================================================================
DDL Script: Create Bronze Tables
===================================================================
Script Purpose:
    This script defines and creates all necessary tables in the 'bronze' schema.
    If any of the tables already exist, they will be dropped before being re-created.
    Use this script to reset the DDL structure for all 'bronze' layer tables.
===================================================================
*/

IF OBJECT_ID('bronze.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;
GO

CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gender NVARCHAR(50),
    cst_create_date DATE
);
GO


IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;
GO

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(10),
    prd_name NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);
GO


IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO


CREATE TABLE bronze.crm_sales_details (
    ssord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
GO


IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

CREATE TABLE bronze.erp_loc_a101 (
    cid NVARCHAR(50),
    entry NVARCHAR(50)
);
GO


IF OBJECT_ID('bronze.erp_cust_a212', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_a212;
GO

CREATE TABLE bronze.erp_cust_a212 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(50)
);
GO


IF OBJECT_ID('bronze.erp_px_cat_giv2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_giv2;
GO

CREATE TABLE bronze.erp_px_cat_giv2 (
    cat NVARCHAR(10),
    id NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50)
);
GO
