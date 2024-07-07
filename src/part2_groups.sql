CREATE OR REPLACE FUNCTION get_all_trans_count(customerId BIGINT, startDate TIMESTAMP, endDate TIMESTAMP)
    RETURNS INTEGER
AS
$$
DECLARE
    trans_count INTEGER;
BEGIN
    SELECT COUNT(DISTINCT tt."Transaction_ID")
    INTO trans_count
    FROM purchase_history ph
             JOIN transactions tt ON ph.transaction_id = tt."Transaction_ID"
    WHERE tt."Transaction_DateTime" between startDate and endDate
      AND ph.customer_id = customerId; -- условие на customer_id в функции

    RETURN trans_count;
END;
$$ LANGUAGE plpgsql;

-- Создание материализованного представления Groups
DROP MATERIALIZED VIEW IF EXISTS Groups CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS Groups AS
-- Общий CTE для выборки данных о покупках клиентов и группах
WITH Purchase_Period AS (SELECT Purchase_History.customer_id,
                                Transaction_id,
                                Transaction_DateTime,
                                Purchase_History.group_id,
                                Group_Cost,
                                Group_Summ,
                                Group_Summ_Paid,
                                First_Group_Purchase_Date,
                                Last_Group_Purchase_Date,
                                Group_Purchase,
                                Group_Frequency,
                                Group_Min_Discount
                         FROM Purchase_History
                                  JOIN Periods ON Periods.customer_id = Purchase_History.customer_id AND
                                                  Periods.group_id = Purchase_History.group_id),
-- Расчет востребованности
     Affinity_Index AS (
         -- CTE для расчета индекса востребованности группы
         WITH COUN AS (SELECT customer_id,
                              COUNT(DISTINCT transaction_id) AS all_transaction
                       FROM Purchase_Period
                       WHERE transaction_datetime BETWEEN first_group_purchase_date AND last_group_purchase_date
                       GROUP BY customer_id)
         SELECT Purchase_Period.customer_id,
                Purchase_Period.group_id,
                group_purchase /
                get_all_trans_count(Purchase_Period.customer_id, Purchase_Period.first_group_purchase_date::TIMESTAMP,
                                    Purchase_Period.last_group_purchase_date::TIMESTAMP)::NUMERIC AS Group_Affinity_Index
         FROM Purchase_Period
                  JOIN COUN ON COUN.customer_id = Purchase_Period.customer_id
         GROUP BY Purchase_Period.customer_id, Purchase_Period.group_id, Group_Affinity_Index),
-- Расчет индекса оттока из группы и фактической маржи по группе для клиента
     Churn_Rate AS (SELECT customer_id,
                           group_id,
                           ((EXTRACT(epoch FROM (SELECT * FROM analysformationdate)) -
                             EXTRACT(epoch FROM MAX(transaction_datetime))) / (group_frequency) /
                            86400::NUMERIC)                  AS Group_Churn_Rate,
                           SUM(Group_Summ_Paid - group_cost) AS Group_Margin
                    FROM Purchase_Period
                    GROUP BY customer_id, group_id, group_frequency),
-- Расчет интервалов потребления группы
     Intervals AS (SELECT customer_id,
                          transaction_id,
                          group_id,
                          transaction_datetime,
                          EXTRACT(DAY FROM (transaction_datetime - LAG(transaction_datetime)
                                                                   OVER (PARTITION BY customer_id, group_id ORDER BY transaction_datetime))) AS interval
                   FROM Purchase_Period
                   GROUP BY customer_id, transaction_id, group_id, transaction_datetime
                   ORDER BY customer_id, transaction_datetime),
-- Расчет стабильности потребления группы
     Stable_Consumption AS (SELECT Intervals.customer_id,
                                   Intervals.group_id,
                                   COALESCE(AVG(CASE
                                                    WHEN Intervals.interval - Purchase_Period.group_frequency < 0::NUMERIC
                                                        THEN (Intervals.interval - Purchase_Period.group_frequency) * -1::NUMERIC
                                                    ELSE Intervals.interval - Purchase_Period.group_frequency
                                                    END / Purchase_Period.group_frequency), 0) AS Group_Stability_Index
                            FROM Intervals
                                     JOIN Purchase_Period
                                          ON Intervals.customer_id = Purchase_Period.customer_id AND
                                             Purchase_Period.group_id = Intervals.group_id
                            GROUP BY Intervals.customer_id, Intervals.group_id),
-- Определение количества транзакций клиента со скидкой
     Count_Discount AS (SELECT PersonalInformation."Customer_ID",
                               ProductGrid."Group_ID",
                               COUNT(DISTINCT Checks."Transaction_ID")
                        FROM PersonalInformation
                                 JOIN Cards ON PersonalInformation."Customer_ID" = Cards."Customer_ID"
                                 JOIN Transactions ON Cards."Customer_Card_ID" = Transactions."Customer_Card_ID"
                                 JOIN Checks ON Transactions."Transaction_ID" = Checks."Transaction_ID"
                                 JOIN ProductGrid ON ProductGrid."SKU_ID" = Checks."SKU_ID"
                        WHERE Checks."SKU_Discount" > 0
                        GROUP BY PersonalInformation."Customer_ID", ProductGrid."Group_ID"),
-- Определение доли транзакций клиента со скидкой от общего количества транзакций по группе
     Count_Discount_Share AS (SELECT Count_Discount."Customer_ID",
                                     Count_Discount."Group_ID",
                                     Count_Discount.count::NUMERIC / Periods.group_purchase::NUMERIC AS Group_Discount_Share
                              FROM Count_Discount
                                       JOIN Periods ON Count_Discount."Group_ID" = Periods.group_id and
                                                       Count_Discount."Customer_ID" = Periods.customer_id
                              GROUP BY Count_Discount."Customer_ID", Count_Discount."Group_ID", Group_Discount_Share),
-- Определение минимального размера скидки по группе
     Min_Discount AS (SELECT customer_id,
                             group_id,
                             MIN(group_min_discount)::NUMERIC AS Group_Minimum_Discount
                      FROM Periods
                      WHERE group_min_discount > 0
                      GROUP BY customer_id, group_id),
-- Определение среднего размера скидки по группе
     Average_discount_amount AS (SELECT customer_id,
                                        group_id,
                                        AVG(group_summ_paid / group_summ)::NUMERIC AS Group_Average_Discount
                                 FROM Purchase_Period
                                          JOIN Checks ON Purchase_Period.transaction_id = Checks."Transaction_ID"
                                 WHERE "SKU_Discount" > 0
                                 GROUP BY customer_id, group_id)
-- Выборка итоговых данных
SELECT Affinity_Index.customer_id AS Customer_ID,            -- Идентификатор клиента
       Affinity_Index.group_id    AS Group_ID,               -- Идентификатор группы
       group_affinity_index       AS group_affinity_index,   -- Индекс востребованности
       Group_Churn_Rate           AS Group_Churn_Rate,       -- Индекс оттока
       Group_Stability_Index      AS Group_Stability_Index,  -- Индекс стабильности
       Group_Margin               AS Group_Margin,           -- Актуальная маржа по группе
       Group_Discount_Share       AS Group_Discount_Share,   -- Доля транзакций со скидкой
       Group_Minimum_Discount     AS Group_Minimum_Discount, -- Минимальный размер скидки
       Group_Average_Discount     AS Group_Average_Discount  -- Средний размер скидки
FROM Affinity_Index
--         JOIN Purchase_Period pp ON Affinity_Index.group_id = pp.group_id AND Affinity_Index.customer_id = pp.group_id
         JOIN Churn_Rate
              ON Affinity_Index.group_id = Churn_Rate.group_id AND Affinity_Index.customer_id = Churn_Rate.customer_id
         JOIN Stable_Consumption ON Stable_Consumption.group_id = Affinity_Index.group_id AND
                                    Stable_Consumption.customer_id = Affinity_Index.customer_id
         JOIN Count_Discount_Share ON Count_Discount_Share."Group_ID" = Affinity_Index.group_id AND
                                      Count_Discount_Share."Customer_ID" = Affinity_Index.customer_id
         JOIN Min_Discount ON Min_Discount.group_id = Affinity_Index.group_id AND
                              Min_Discount.customer_id = Affinity_Index.customer_id
         JOIN Average_discount_amount ON Affinity_Index.group_id = Average_discount_amount.group_id AND
                                         Affinity_Index.customer_id = Average_discount_amount.customer_id;


-- Тестовые запросы для материализованного представления Groups
SELECT *
FROM Groups
where Group_ID = 7
  and Customer_ID = 1;

SELECT *
FROM Groups;

SELECT *
FROM Groups
WHERE Group_Average_Discount > 0.94;

SELECT *
FROM Groups
WHERE customer_id = 8;

SELECT *
FROM Groups
WHERE group_margin < 0;

SELECT *
FROM Groups
WHERE group_margin > 2000;

SELECT *
FROM Groups
WHERE customer_id = 1;