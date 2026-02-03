## Incremental Data Handling

The HR pipeline is fully **incremental**, meaning it only processes **new data** since the last pipeline run:

1. **Bronze Layer**  
   - Appends **new CSV rows** into `bronze.hr_employee_raw`.  
   - `LOAD_DATE` column tracks when each row was ingested.  
   - Old rows are **never deleted or overwritten**.

2. **Silver Layer**  
   - Processes only **new records** from Bronze where `LOAD_DATE > MAX(LOAD_DATE)` already in Silver.  
   - Deduplicates employees by keeping only the **latest record per EMPID**.  
   - Normalizes columns such as `SEX` and `TERMED` → `IS_TERMINATED`.

3. **Gold Layer**  
   - **Dimension Table (`dim_employee`)**: Inserts only new employees not already in Gold.  
   - **Fact Tables (`fact_headcount`, `fact_attrition`)**: Inserts **daily snapshots** of active and terminated employees.  
   - Historical snapshots are preserved; only new data is added each day.

**Benefit:**  
- No need to rebuild Silver or Gold from scratch.  
- Efficient for large datasets.  
- Ensures daily HR reports and analytics are always up-to-date.
