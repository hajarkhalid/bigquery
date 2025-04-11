WITH SalesData AS (
    SELECT
        sale_id,
        customer_id,
        sale_date,
        sale_amount,
        product_id
    FROM
        `your_project_id.your_dataset_id.sales`
    WHERE 
        sale_date BETWEEN '2024-01-01' AND '2024-12-31'
),

CustomerData AS (
    SELECT
        customer_id,
        customer_name,
        customer_email,
        membership_start_date
    FROM
        `your_project_id.your_dataset_id.customers`
)

SELECT
    sd.sale_id,
    sd.customer_id,
    cd.customer_name,
    cd.customer_email,
    sd.product_id,
    sd.sale_amount
FROM 
    SalesData sd
LEFT JOIN 
    CustomerData cd
    ON sd.customer_id = cd.customer_id
    AND sd.sale_date >= cd.membership_start_date  -- Multiple conditions combined with AND
