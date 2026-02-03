CREATE OR REPLACE PROCEDURE bronze.sp_load_hr_employee()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

    COPY INTO bronze.hr_employee_raw
    FROM (
        SELECT
            t.*,
            CURRENT_DATE
        FROM @hr_stage t
    )
    ON_ERROR = CONTINUE;

    RETURN 'Bronze HR load completed';

END;
$$;
