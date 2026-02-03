CREATE TABLE IF NOT EXISTS silver.hr_employee_clean (
    EMPID INT,
    EMPLOYEE_NAME STRING,
    GENDER STRING,
    POSITION STRING,
    DEPARTMENT STRING,
    STATE STRING,
    SALARY NUMBER,
    IS_TERMINATED BOOLEAN,
    EMPLOYMENTSTATUS STRING,
    DATEOFHIRE DATE,
    DATEOFTERMINATION DATE,
    LOAD_DATE DATE
);



CREATE OR REPLACE PROCEDURE silver.sp_build_hr_employee_clean()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    INSERT INTO silver.hr_employee_clean
    SELECT
        EMPID,
        TRIM(EMPLOYEE_NAME),

        CASE
            WHEN SEX = 'M' THEN 'MALE'
            WHEN SEX = 'F' THEN 'FEMALE'
            ELSE 'UNKNOWN'
        END,

        POSITION,
        DEPARTMENT,
        STATE,
        SALARY,

        CASE
            WHEN TERMED = 1 THEN TRUE
            ELSE FALSE
        END,

        EMPLOYMENTSTATUS,
        DATEOFHIRE,
        DATEOFTERMINATION,
        LOAD_DATE
    FROM bronze.hr_employee_raw
    WHERE LOAD_DATE >
        COALESCE(
            (SELECT MAX(LOAD_DATE) FROM silver.hr_employee_clean),
            '1900-01-01'
        );

    RETURN 'Silver HR incremental clean completed';

END;
$$;




