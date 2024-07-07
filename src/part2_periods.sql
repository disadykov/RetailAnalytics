-- Создание материализованного представления Periods
DROP MATERIALIZED VIEW IF EXISTS Periods CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS Periods AS
-- Общий CTE для выборки данных о покупках клиентов и скидках по группам
WITH Period AS (SELECT PersonalInformation."Customer_ID",
                       Transactions."Transaction_ID",
                       ProductGrid."Group_ID",
                       (Checks."SKU_Discount" / Checks."SKU_Summ") AS Group_Min_Discount
                FROM PersonalInformation
                         JOIN Cards ON PersonalInformation."Customer_ID" = Cards."Customer_ID"
                         JOIN Transactions ON Cards."Customer_Card_ID" = Transactions."Customer_Card_ID"
                         JOIN Checks ON Checks."Transaction_ID" = Transactions."Transaction_ID"
                         JOIN ProductGrid ON Checks."SKU_ID" = ProductGrid."SKU_ID"
                         JOIN Stores ON ProductGrid."SKU_ID" = Stores."SKU_ID" AND
                                        Stores."Transaction_Store_ID" = Transactions."Transaction_Store_ID"
                GROUP BY PersonalInformation."Customer_ID", ProductGrid."Group_ID", Transactions."Transaction_ID",
                         Group_Min_Discount),
-- CTE для определения первой и последней покупки каждой группы для каждого клиента
     First_Last_Group_Purchase AS (SELECT customer_id,
                                          MIN(Transaction_DateTime) AS First_Group_Purchase_Date,
                                          MAX(Transaction_DateTime) AS Last_Group_Purchase_Date,
                                          group_id,
                                          COUNT(Transaction_ID)     AS Group_Purchase
                                   FROM Purchase_History
                                   GROUP BY customer_id, group_id),
-- CTE для определения частоты покупок каждой группы для каждого клиента
     Group_Frequency AS (SELECT customer_id,
                                group_id,
                                ((EXTRACT(EPOCH FROM Last_Group_Purchase_Date - First_Group_Purchase_Date) / 86400 +
                                  1) / Group_Purchase) AS Group_Frequency
                         FROM First_Last_Group_Purchase)
-- Выборка итоговых данных
SELECT First_Last_Group_Purchase.Customer_ID AS Customer_ID,               -- Идентификатор клиента
       First_Last_Group_Purchase.Group_ID    AS Group_ID,                  -- Идентификатор группы SKU
       First_Group_Purchase_Date             AS First_Group_Purchase_Date, -- Дата первой покупки группы
       Last_Group_Purchase_Date              AS Last_Group_Purchase_Date,  -- Дата последней покупки группы
       Group_Purchase                        AS Group_Purchase,            -- Количество транзакций с группой
       ROUND(Group_Frequency, 2)             AS Group_Frequency,           -- Интенсивность покупок группы
       CASE
           WHEN MAX(group_min_discount) = 0 THEN 0 -- Минимальный размер скидки по группе
           ELSE (MIN(Group_Min_Discount) FILTER ( WHERE group_min_discount > 0 ))
           END                               AS Group_Min_Discount
FROM Period
         JOIN First_Last_Group_Purchase
              ON First_Last_Group_Purchase.customer_id = Period."Customer_ID" AND
                 Period."Group_ID" = First_Last_Group_Purchase.group_id
         JOIN Group_Frequency
              ON Group_Frequency.customer_id = First_Last_Group_Purchase.customer_id AND
                 Group_Frequency.group_id = Period."Group_ID"
GROUP BY First_Last_Group_Purchase.group_id, First_Last_Group_Purchase.customer_id, First_Group_Purchase_Date,
         Last_Group_Purchase_Date,
         Group_Purchase, Group_Frequency
ORDER BY First_Last_Group_Purchase.customer_id, First_Last_Group_Purchase.group_id;

-- Тестовые запросы для материализованного представления Periods
SELECT *
FROM Periods;

SELECT *
FROM Periods
WHERE Group_Min_Discount = 0;

SELECT *
FROM Periods
WHERE Group_Purchase > 7;

SELECT *
FROM Periods
WHERE Group_ID > 3
  AND Group_ID < 6;

SELECT *
FROM Periods
ORDER BY 4 DESC;

SELECT *
FROM Periods
WHERE customer_id = 9
ORDER BY 4 DESC;
