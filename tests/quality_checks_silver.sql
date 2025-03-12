-- CHECK DATA QUALITY FOR SILVER.ERP_CUST_AZ12

-- Identify Out-of-range Dates

select distinct
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate(); -- bad data quality result

select distinct
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate();

-- Data Standariization and Consistency
select distinct gen
from silver.erp_cust_az12;

select distinct gen
from silver.erp_cust_az12;

select distinct
	gen,
	case when upper(trim(gen)) in ('F', 'Female') then 'Female'
		when upper(trim(gen)) in ('M', 'Male') then 'Male'
		else 'n/a'
	END AS gen
from silver.erp_cust_az12;


-- CHECK DATA QUALITY FO SILVER.ERP_LOC_A101

-- Data Standardization and Consistency
-- cntry
select distinct cntry
from silver.erp_loc_a101
order by cntry;

select distinct cntry as old_cntry,
	case when trim(cntry) = 'DE' then 'Germany'
		when trim(cntry) in ('US', 'USA') then 'United States'
		when trim(cntry) = '' or cntry is null then 'n/a'
		else trim(cntry)
	end as cntry
from silver.erp_loc_a101
order by cntry;


-- CHECK DATA QUALITY FO SILVER.ERP_PX_CAT_G1V2
-- check for unwanted spaces
select *
from silver.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance) or id != trim(id)

-- Data Standardization and consistency

select distinct cat
from silver.erp_px_cat_g1v2;

select distinct subcat
from silver.erp_px_cat_g1v2;

select distinct maintenance
from silver.erp_px_cat_g1v2;
--- Everything looks nice---
