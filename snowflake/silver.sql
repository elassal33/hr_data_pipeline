CREATE OR REPLACE PROCEDURE silver.sp_build_hr_employee_clean()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    CREATE OR REPLACE TABLE silver.hr_employee_clean AS
    SELECT
        EMPID,
        TRIM(EMPLOYEE_NAME) AS EMPLOYEE_NAME,

        CASE
            WHEN SEX = 'M' THEN 'MALE'
            WHEN SEX = 'F' THEN 'FEMALE'
            ELSE 'UNKNOWN'
        END AS GENDER,

        POSITION,
        DEPARTMENT,
        STATE,
        SALARY,

        CASE
            WHEN TERMED = 1 THEN TRUE
            ELSE FALSE
        END AS IS_TERMINATED,

        EMPLOYMENTSTATUS,
        DATEOFHIRE,
        DATEOFTERMINATION,
        LOAD_DATE
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY EMPID
                   ORDER BY LOAD_DATE DESC
               ) AS rn
        FROM bronze.hr_employee_raw
    )
    WHERE rn = 1;

    RETURN 'Silver HR clean completed';

END;
$$;
