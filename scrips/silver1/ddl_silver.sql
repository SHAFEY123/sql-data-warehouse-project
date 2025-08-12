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
-------------------------------------------------------------------
-- silver.crm_cust_info
-------------------------------------------------------------------
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    cst_id NVARCHAR(50),
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(100),
    cst_lastname NVARCHAR(100),
    cst_marital_status NVARCHAR(20),
    cst_gender NVARCHAR(20),
    cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-------------------------------------------------------------------
-- silver.crm_prd_info
-------------------------------------------------------------------
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id NVARCHAR(50),
    prd_key NVARCHAR(50),
    cat_id NVARCHAR(50),
    prd_nm NVARCHAR(255),
    prd_cost DECIMAL(18,2),
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-------------------------------------------------------------------
-- silver.crm_sales_details
-------------------------------------------------------------------
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_id DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-------------------------------------------------------------------
-- silver.erp_cust_a212
-------------------------------------------------------------------
IF OBJECT_ID('silver.erp_cust_a212', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_a212;

CREATE TABLE silver.erp_cust_a212 (
    cid NVARCHAR(50),
    bdate DATE,
    gen NVARCHAR(20),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-------------------------------------------------------------------
-- silver.erp_loc_a101
-------------------------------------------------------------------
IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-------------------------------------------------------------------
-- silver.erp_px_cat_g1v2
-------------------------------------------------------------------
IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id NVARCHAR(50),
    cat NVARCHAR(50),
    subcat NVARCHAR(50),
    maintenance NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
