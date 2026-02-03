CREATE TABLE IF NOT EXISTS gold.dim_employee (
    EMPID INT,
    EMPLOYEE_NAME STRING,
    GENDER STRING,
    POSITION STRING,
    DEPARTMENT STRING,
    STATE STRING,
    DATEOFHIRE DATE,
    LOAD_DATE DATE
);

CREATE OR REPLACE PROCEDURE gold.sp_build_dim_employee()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    INSERT INTO gold.dim_employee
    SELECT
        s.EMPID,
        s.EMPLOYEE_NAME,
        s.GENDER,
        s.POSITION,
        s.DEPARTMENT,
        s.STATE,
        s.DATEOFHIRE,
        s.LOAD_DATE
    FROM silver.hr_employee_clean s
    LEFT JOIN gold.dim_employee d
      ON s.EMPID = d.EMPID
    WHERE d.EMPID IS NULL;

    RETURN 'Gold dim_employee incremental load completed';

END;
$$;


CREATE TABLE IF NOT EXISTS gold.fact_headcount (
    SNAPSHOT_DATE DATE,
    DEPARTMENT STRING,
    HEADCOUNT INT
);

CREATE OR REPLACE PROCEDURE gold.sp_build_fact_headcount()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    INSERT INTO gold.fact_headcount
    SELECT
        CURRENT_DATE,
        DEPARTMENT,
        COUNT(*)
    FROM silver.hr_employee_clean
    WHERE IS_TERMINATED = FALSE
    GROUP BY DEPARTMENT;

    RETURN 'Gold fact_headcount snapshot inserted';

END;
$$;

CREATE TABLE IF NOT EXISTS gold.fact_attrition (
    SNAPSHOT_DATE DATE,
    DEPARTMENT STRING,
    TERMINATED_EMPLOYEES INT
);

CREATE OR REPLACE PROCEDURE gold.sp_build_fact_attrition()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    INSERT INTO gold.fact_attrition
    SELECT
        CURRENT_DATE,
        DEPARTMENT,
        COUNT(*)
    FROM silver.hr_employee_clean
    WHERE IS_TERMINATED = TRUE
    GROUP BY DEPARTMENT;

    RETURN 'Gold fact_attrition snapshot inserted';

END;
$$;
