DROP FUNCTION IF EXISTS growth_oriented_margin;
CREATE OR REPLACE FUNCTION growth_oriented_margin (nums_of_groups INT, max_churn_index FLOAT, max_stability_index FLOAT, sku_percentage FLOAT, margin_percentage FLOAT)
RETURNS TABLE (customer_id BIGINT, sku_name VARCHAR, offer_discount_depth NUMERIC)
AS $$
BEGIN
RETURN QUERY WITH 
-- Выбор группы
    groups_for_offer AS (
        SELECT n_groups.customer_id, n_groups.group_id, n_groups.customer_primary_store, n_groups.min_discount
        FROM (
            SELECT 
            g.customer_id,
            g.group_id,
            g.group_affinity_index,
            g.group_churn_rate,
            g.group_stability_index,
            c.customer_primary_store,
            ROUND(CASE WHEN g.group_minimum_discount = 0 THEN 0.05 ELSE CEIL(g.group_minimum_discount * 20) / 20 END, 2) AS min_discount,
            ROW_NUMBER() OVER (PARTITION BY g.customer_id ORDER BY g.group_affinity_index DESC) AS Rank
        FROM "groups" g
        JOIN customers c
        ON g.customer_id = c.customer_id
        WHERE
            g.group_churn_rate <= max_churn_index AND
            g.group_stability_index < max_stability_index) AS n_groups
        WHERE n_groups.Rank <= nums_of_groups),
-- Определение SKU с максимальной маржой
    sku_max_margin AS (
        SELECT  
            sku_margin."Transaction_Store_ID", 
            sku_margin."SKU_ID", 
            sku_margin."Group_ID", 
            sku_margin.margin,
            sku_margin.max_discount,
            ROW_NUMBER() OVER (PARTITION BY sku_margin."Group_ID" ORDER BY sku_margin.margin DESC) AS nums
        FROM (
            SELECT 
                s."Transaction_Store_ID", 
                s."SKU_ID", 
                p."Group_ID", 
                (s."SKU_Retail_Price" - s."SKU_Purchase_Price") AS margin,
                (((s."SKU_Retail_Price" - s."SKU_Purchase_Price") / s."SKU_Retail_Price") * margin_percentage / 100) AS max_discount
            FROM stores s
            JOIN productgrid p
            ON s."SKU_ID" = p."SKU_ID"
            JOIN skugroup sg
            ON p."Group_ID" = sg."Group_ID") AS sku_margin),
-- Определение доли SKU в группе
    sku_share AS (
        SELECT
            sm."Transaction_Store_ID", 
            sm."SKU_ID", 
            sm."Group_ID",
            (SELECT COUNT(*) 
                FROM checks ch 
                JOIN transactions tr 
                ON ch."Transaction_ID" = tr."Transaction_ID"
                WHERE ch."SKU_ID" = sm."SKU_ID")::NUMERIC /
            (SELECT COUNT(*)
                FROM purchase_history ph 
                JOIN transactions tr 
                ON ph.transaction_id = tr."Transaction_ID"
                WHERE ph.group_id = sm."Group_ID")::NUMERIC AS s_share
        FROM sku_max_margin sm
        WHERE sm.nums = 1)

SELECT DISTINCT gr.customer_id, pg."SKU_Name", ROUND(gr.min_discount * 100) AS offer_discount_depth
FROM groups_for_offer gr
JOIN productgrid pg
ON gr.group_id = pg."Group_ID"
JOIN sku_max_margin sm
ON sm."SKU_ID" = pg."SKU_ID"
JOIN sku_share ss
ON ss."SKU_ID" = pg."SKU_ID"
WHERE gr.min_discount < sm.max_discount AND
ss.s_share <= sku_percentage / 100;
END;
$$ LANGUAGE plpgsql;


-- Проверка
SELECT * FROM growth_oriented_margin (5, 5, 0.7, 100, 60);
SELECT * FROM growth_oriented_margin (5, 3, 0.5, 100, 30);
SELECT * FROM growth_oriented_margin (2, 1, 1, 100, 20);