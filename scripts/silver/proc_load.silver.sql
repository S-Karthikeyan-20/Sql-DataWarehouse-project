CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME ,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
    BEGIN TRY
    SET @batch_start_time =GETDATE();

    PRINT'========================================================';
    PRINT 'Loading Silver Layer';
    PRINT'========================================================';

    PRINT '-------------------------------------------------------';
    PRINT 'Loading CRM Table';
    PRINT '-------------------------------------------------------';


    SET @start_time = GETDATE();
    PRINT'>> Truncating Table: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info
    PRINT'>> Inserting Table: silver.crm_cust_info';

    INSERT INTO silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date )

    select cst_id,
    cst_key,
    trim(cst_firstname) as cst_firstname,
    trim(cst_lastname) as cst_lastname,

    case when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
         when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
         else 'n/a'
    end cst_marital_status,

    case when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
         when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
         else 'n/a'
    end cst_gndr,
    cst_create_date 
    from
    (
    select * ,
    ROW_NUMBER() over(partition by cst_id order by cst_create_date DESC) as rn
    from bronze.crm_cust_info
    where cst_id is not null
    ) t
    where rn = 1;
   
    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'seconds';

    PRINT'------------------------------------------------------';
    SET @start_time = GETDATE();

    PRINT'>> Truncating Table: silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info
    PRINT'>> Inserting Table: silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info(
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
    )

    select 
    prd_id,
    replace(substring(prd_key,1,5),'-','_') as cat_id ,
    substring(prd_key,7,len(prd_key)) as prd_key,
    prd_nm,
    isnull(prd_cost,0) as prd_cost,

    case when upper(trim(prd_line)) ='M' then 'Mountain'
         when upper(trim(prd_line)) ='R' then 'Road'
         when upper(trim(prd_line)) ='T' then 'Touring'
         when upper(trim(prd_line)) ='S' then 'Other sales'
         else 'n/a'
    end as prd_line,

    cast(prd_start_dt as DATE) as prd_start_dt,
    CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) as prd_end_dt
    from 
    bronze.crm_prd_info;

    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'seconds';

    PRINT'------------------------------------------------------';
    SET @start_time = GETDATE();
    PRINT'>> Truncating Table: silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details
    PRINT'>> Inserting Table: silver.crm_sales_details';
    INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
        )
    select sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    case when sls_order_dt =0 OR LEN(sls_order_dt)!=8 then null
         ELSE  cast(cast(sls_order_dt as varchar) as DATE)
    end as sls_order_dt,

    case when sls_ship_dt =0 OR LEN(sls_ship_dt)!=8 then null
         ELSE  cast(cast(sls_ship_dt as varchar) as DATE)
    end as sls_ship_dt,

    case when sls_due_dt =0 OR LEN(sls_due_dt)!=8 then null
         ELSE  cast(cast(sls_due_dt as varchar) as DATE)
    end as sls_due_dt,

    case when sls_sales is null  or sls_sales <0 or sls_sales != sls_quantity* ABS(sls_price) then 
               sls_quantity* ABS(sls_price)
        else sls_sales
    end as sls_sales,

    sls_quantity,

    case when sls_price  is null  or sls_price <0  then 
               sls_sales/ NULLIF(sls_quantity,0)
        else sls_price 
    end as sls_price 

    from
    bronze.crm_sales_details;

    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'seconds';

     PRINT '-------------------------------------------------------';
     PRINT 'Loading ERP Table';
     PRINT '-------------------------------------------------------';

    SET @start_time = GETDATE();
    PRINT'>> Truncating Table: silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12
    PRINT'>> Inserting Table: silver.erp_cust_az12';


    INSERT INTO silver.erp_cust_az12(cid,bdate,gen)

    select

    case when cid LIKE  'NAS%' then substring(cid,4,LEN(CID))
         ELSE cid
    end as cid,

    case when bdate > GETDATE() THEN NULL
         else bdate
    end as bdate,

    case when UPPER(TRIM(gen)) IN ('F' ,'Female') then 'Female'
         when UPPER(TRIM(gen)) IN ('M' ,'Male') then 'Male'
         else 'n/a'
    end as gen

    from bronze.erp_cust_az12;

    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'seconds';

    PRINT'------------------------------------------------------';
    SET @start_time = GETDATE();
    PRINT'>> Truncating Table: silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101
    PRINT'>> Inserting Table: silver.erp_loc_a101';



    INSERT INTO silver.erp_loc_a101(cid,cntry)

    select 
    replace(cid,'-' , '')as cid,

    case when trim(cntry) = 'DE' then 'Germany'
         when trim(cntry) in ('US','USA') then 'United States'
         when trim(cntry) = '' or trim(cntry) is null then 'n/a'
         else trim(cntry)
    end as cntry
    from bronze.erp_loc_a101;

    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'seconds';

    PRINT'------------------------------------------------------';

    SET @start_time = GETDATE();
    PRINT'>> Truncating Table: silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2
    PRINT'>> Inserting Table: silver.erp_px_cat_g1v2';

    INSERT INTO silver.erp_px_cat_g1v2(id,
    cat,
    subcat,
    maintenance
    )

    select id,
    cat,
    subcat,
    maintenance
    from 
    bronze.erp_px_cat_g1v2

    SET @end_time = GETDATE();
    PRINT'>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR) +'seconds';
    PRINT'------------------------------------------------------';

    SET @batch_end_time =GETDATE();

    PRINT'========================================================';
    PRINT'Loading Silver Layer is completed';
    PRINT'>> Total Load Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS NVARCHAR) +'seconds';
    PRINT'========================================================';


    END TRY
    BEGIN CATCH
         PRINT'========================================================';
         PRINT'ERROR OCCURED DURING LOADING SILVER LAYER';
         PRINT'Error Message' + ERROR_MESSAGE();
         PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
         PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
         PRINT'========================================================';
    END CATCH

END

