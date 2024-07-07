DROP FUNCTION IF EXISTS growth_orient_offer;
CREATE OR REPLACE FUNCTION growth_orient_offer (first_date TIMESTAMP, last_date TIMESTAMP, additional_transactions NUMERIC, churn_index FLOAT, max_discount_percentage FLOAT, margin_percentage FLOAT)
RETURNS TABLE (customer_id BIGINT, start_date TIMESTAMP, end_date TIMESTAMP, required_transactions_count NUMERIC, group_name VARCHAR, offer_discount_depth NUMERIC)
AS $$
BEGIN
RETURN QUERY WITH 
-- Определение текущей частоты посещений клиента в заданный период
    frequency AS (
        SELECT c.customer_id, (((date(last_date) - date(first_date))::NUMERIC / customer_frequency)) AS frequency_of_visits
        FROM customers c),
-- Определение транзакции для начисления вознаграждения
    num_transaction AS (
        SELECT f.customer_id, (ROUND(f.frequency_of_visits) + additional_transactions) AS amount_transaction
        FROM frequency f),
-- Определение средней маржи по группе
    avg_margin AS (
        SELECT ph.customer_id, ph.group_id, ((SUM(group_summ_paid) - SUM(group_cost)) / SUM(group_summ_paid)) AS margin
        FROM purchase_history ph
        GROUP BY ph.customer_id, ph.group_id),   
-- Определение группы для формирования вознаграждения
    reward_group AS (
        SELECT subquery.customer_id, subquery.group_id, min_discount
        FROM (  
            SELECT 
                g.customer_id,
                g.group_id,
                g.group_affinity_index,
                g.group_churn_rate,
                g.group_discount_share,
                g.group_margin,
                ROUND(CASE WHEN g.group_minimum_discount = 0 THEN 0.05 ELSE CEIL(g.group_minimum_discount * 20) / 20 END, 2) AS min_discount,
                avg_margin.margin,
                ROW_NUMBER() OVER (PARTITION BY g.customer_id ORDER BY g.group_affinity_index DESC) AS Rank
                FROM groups g
                JOIN avg_margin 
                ON g.customer_id = avg_margin.customer_id AND g.group_id = avg_margin.group_id
            WHERE g.group_churn_rate <= churn_index
                AND g.group_discount_share < (max_discount_percentage / 100)
                AND g.group_minimum_discount < avg_margin.margin * margin_percentage / 100
                ) AS subquery
        WHERE subquery.Rank = 1)
    
SELECT rg.customer_id, first_date AS start_date, last_date AS end_date, nt.amount_transaction AS required_transactions_count, sg."Group_Name", ROUND(rg.min_discount * 100) AS offer_discount_depth
FROM reward_group rg
JOIN num_transaction nt
ON rg.customer_id = nt.customer_id 
JOIN skugroup sg
ON rg.group_id = sg."Group_ID";
END;
$$ LANGUAGE plpgsql;


-- Проверка
SELECT * FROM growth_orient_offer ('2024-01-11', '2024-01-25', 1, 2, 80, 50);
SELECT * FROM growth_orient_offer ('2024-01-11', '2024-01-25', 1, 3, 70, 30);
SELECT * FROM growth_orient_offer ('2024-01-11', '2024-01-25', 1, 5, 100, 100);