
/*
    Procedure: bronze.load_bronze
    Purpose: Loads raw data from source CSV files into the bronze layer tables of the data warehouse.
        This procedure handles data ingestion for CRM tables (customer info, product info, and other CRM data)
        using bulk insert operations.
        It includes truncation of existing data, performance tracking, and detailed logging of each load operation.
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    
    DECLARE @start_time DATETIME, @end_time DATETIME, @bronze_start DATETIME, @bronze_end DATETIME;
    BEGIN TRY
        
        PRINT '
=========================================';
        PRINT 'Loading Bronze Layer'
        PRINT '=========================================
        ';

        PRINT '----------------------------------------';
        PRINT 'Loading CRM Tables'
        PRINT '-----------------------------------------
        ';
        SET @bronze_start = GETDATE()

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE [bronze].[crm_cust_info];
        PRINT '>> Insertng Date Into Table: bronze.crm_cust_info';
        BULK INSERT [bronze].[crm_cust_info]
        FROM 'C:\SQL2025\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT '>> Load Duration ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'


        SET @start_time = GETDATE()
        PRINT '
>> Truncating Table: bronze.crm_prd_info'
        TRUNCATE TABLE [bronze].[crm_prd_info];
        PRINT '>> Insertng Date Into Table: bronze.crm_prd_info';
        BULK INSERT [bronze].[crm_prd_info]
        FROM 'C:\SQL2025\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE()
        PRINT '>> Load Duration ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'

        SET @start_time = GETDATE()
        PRINT '
>> Truncating Table: bronze.crm_sales_details'
        TRUNCATE TABLE [bronze].[crm_sales_details];
        PRINT '>> Insertng Date Into Table: bronze.crm_sales_details';
        BULK INSERT [bronze].[crm_sales_details]
        FROM 'C:\SQL2025\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT '>> Load Duration ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'
        PRINT '
----------------------------------------';
        PRINT 'Loading ERP Tables'
        PRINT '-----------------------------------------
        ';


        SET @start_time = GETDATE()
        PRINT '
>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE [bronze].[erp_cust_az12];
        PRINT '>> Insertng Date Into Table: bronze.erp_cust_az12';
        BULK INSERT [bronze].[erp_cust_az12]
        FROM 'C:\SQL2025\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT '>> Load Duration ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'

        SET @start_time = GETDATE()
        PRINT '
>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE [bronze].[erp_loc_a101];
        PRINT '>> Insertng Date Into Table: bronze.erp_loc_a101';
        BULK INSERT [bronze].[erp_loc_a101]
        FROM 'C:\SQL2025\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT '>> Load Duration ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'

        SET @start_time = GETDATE()
        PRINT '
>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE [bronze].[erp_px_cat_g1v2];
        PRINT '>> Insertng Date Into Table: bronze.erp_px_cat_g1v2';
        BULK INSERT [bronze].[erp_px_cat_g1v2]
        FROM 'C:\SQL2025\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE()
        PRINT '>> Load Duration ' + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR) + ' ms'

        SET @bronze_end = GETDATE()
        PRINT '
>> Loading Bronze Layer Duration ' + CAST(DATEDIFF(millisecond, @bronze_start, @bronze_end) AS NVARCHAR) + ' ms'
    PRINT '
Successfull Loading'
    
    END TRY
    
    BEGIN CATCH
        PRINT '============================================';
        print 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '============================================';
    
    END CATCH
END
