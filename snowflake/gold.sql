CREATE OR REPLACE PROCEDURE gold.sp_build_dim_employee()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    CREATE OR REPLACE TABLE gold.dim_employee AS
    SELECT
        EMPID,
        EMPLOYEE_NAME,
        GENDER,
        POSITION,
        DEPARTMENT,
        STATE,
        DATEOFHIRE
    FROM silver.hr_employee_clean;

    RETURN 'Gold dim_employee built';

END;
$$;

CREATE OR REPLACE PROCEDURE gold.sp_build_fact_headcount()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    CREATE OR REPLACE TABLE gold.fact_headcount AS
    SELECT
        CURRENT_DATE AS SNAPSHOT_DATE,
        DEPARTMENT,
        COUNT(*) AS HEADCOUNT
    FROM silver.hr_employee_clean
    WHERE IS_TERMINATED = FALSE
    GROUP BY DEPARTMENT;

    RETURN 'Gold fact_headcount built';

END;
$$;

CREATE OR REPLACE PROCEDURE gold.sp_build_fact_attrition()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    CREATE OR REPLACE TABLE gold.fact_attrition AS
    SELECT
        CURRENT_DATE AS SNAPSHOT_DATE,
        DEPARTMENT,
        COUNT(*) AS TERMINATED_EMPLOYEES
    FROM silver.hr_employee_clean
    WHERE IS_TERMINATED = TRUE
    GROUP BY DEPARTMENT;

    RETURN 'Gold fact_attrition built';

END;
$$;


