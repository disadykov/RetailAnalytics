-- Представление История покупок
DROP MATERIALIZED VIEW IF EXISTS Purchase_History CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS Purchase_History AS
SELECT PersonalInformation."Customer_ID"                               AS Customer_ID,          -- Идентификатор клиента
       Transactions."Transaction_ID"                                   AS Transaction_ID,       -- Идентификатор транзакции
       Transactions."Transaction_DateTime"                             AS Transaction_DateTime, -- Дата транзакции
       ProductGrid."Group_ID"                                          AS Group_ID,             -- Группа SKU
       SUM(Stores."SKU_Purchase_Price" * Checks."SKU_Amount")::NUMERIC AS Group_Cost,           -- Себестоимость
       SUM(Checks."SKU_Summ")::NUMERIC                                 AS Group_Summ,           -- Базовая розничная стоимость
       SUM(Checks."SKU_Summ_Paid") ::NUMERIC                           AS Group_Summ_Paid       -- Фактически оплаченная стоимость
FROM PersonalInformation
         JOIN Cards ON PersonalInformation."Customer_ID" = Cards."Customer_ID"
         JOIN Transactions ON Cards."Customer_Card_ID" = Transactions."Customer_Card_ID"
         JOIN Checks ON Checks."Transaction_ID" = Transactions."Transaction_ID"
         JOIN ProductGrid ON Checks."SKU_ID" = ProductGrid."SKU_ID"
         JOIN Stores
              ON ProductGrid."SKU_ID" = Stores."SKU_ID" AND
                 Stores."Transaction_Store_ID" = Transactions."Transaction_Store_ID"
GROUP BY PersonalInformation."Customer_ID", Transactions."Transaction_ID", Transactions."Transaction_DateTime",
         ProductGrid."Group_ID";

-- Тестовые запросы для материализованного представления Purchase_History
SELECT *
FROM Purchase_History where Group_ID = 7;

SELECT *
FROM Purchase_History
WHERE Group_Cost > 4000;

SELECT *
FROM Purchase_History
WHERE Group_Cost < 1000
  AND Group_Summ_Paid > 800;

SELECT *
FROM Purchase_History
ORDER BY 1, 4;

SELECT *
FROM Purchase_History
WHERE Group_Summ < 43;

SELECT *
FROM Purchase_History
WHERE Customer_ID = 1;