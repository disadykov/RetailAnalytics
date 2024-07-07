-- Создаем новую таблицу Segments с четырьмя колонками:
-- сегмент, средний чек, частота покупок, вероятность оттока.
DROP TABLE IF EXISTS Segments CASCADE;
CREATE TABLE IF NOT EXISTS Segments
(
    Segment                INTEGER PRIMARY KEY NOT NULL,
    Average_check          VARCHAR             NOT NULL,
    Frequency_of_purchases VARCHAR             NOT NULL,
    Churn_probability      VARCHAR             NOT NULL
);

-- Импортируем значения из файла
CALL tsv_import('segments', '/tmp/datasets/Segments.tsv');

-- Это самое основное представление "Customers". Здесь мы создаем
-- последовательность CTE (common table expressions), каждая из которых
-- выполняет определенный шаг анализа данных, такой как вычисление
-- среднего чека, сегментация клиентов и определение основного магазина.
DROP
    MATERIALIZED VIEW IF EXISTS Customers;
CREATE MATERIALIZED VIEW IF NOT EXISTS Customers AS
-- Считаем средний чек для каждого клиента
WITH Id_Average_Check AS (SELECT PersonalInformation."Customer_ID"                              AS customer_id,
                                 (SUM("Transaction_Summ") / COUNT("Transaction_Summ"))::numeric AS customer_average_check
                          FROM PersonalInformation
                                   JOIN Cards ON PersonalInformation."Customer_ID" = Cards."Customer_ID"
                                   JOIN Transactions ON Cards."Customer_Card_ID" = Transactions."Customer_Card_ID"
                          GROUP BY PersonalInformation."Customer_ID"
                          ORDER BY customer_average_check DESC),
-- Сегментируем клиентов на основе среднего чека
     Average_Check_Segment AS (SELECT customer_id,
                                      customer_average_check,
                                      CASE
                                          WHEN PERCENT_RANK() OVER (ORDER BY customer_average_check DESC) <= 0.1
                                              THEN 'High'
                                          WHEN PERCENT_RANK() OVER (ORDER BY customer_average_check DESC) <= 0.35
                                              THEN 'Medium'
                                          ELSE 'Low'
                                          END AS customer_average_check_segment
                               FROM Id_Average_Check),
-- Считаем частоту покупок для каждого клиента
     Frequency AS (SELECT PersonalInformation."Customer_ID" AS customer_id,
                          (MAX(date("Transaction_DateTime")) - MIN(date("Transaction_DateTime"))) /
                          COUNT("Transaction_ID")::numeric  AS customer_frequency
                   FROM PersonalInformation
                            JOIN Cards ON PersonalInformation."Customer_ID" = Cards."Customer_ID"
                            JOIN Transactions ON Cards."Customer_Card_ID" = Transactions."Customer_Card_ID"
                   GROUP BY PersonalInformation."Customer_ID"
                   ORDER BY customer_frequency),
-- Сегментируем клиентов на основе частоты покупок
     Frequency_Segment AS (SELECT customer_id,
                                  customer_frequency,
                                  CASE
                                      WHEN PERCENT_RANK() OVER (ORDER BY customer_frequency ASC) <= 0.1 THEN 'Often'
                                      WHEN PERCENT_RANK() OVER (ORDER BY customer_frequency ASC) <= 0.35
                                          THEN 'Occasionally'
                                      ELSE 'Rarely'
                                      END AS customer_frequency_segment
                           FROM Frequency),
-- Находим период неактивности для каждого клиента
     Inactive_Period AS (SELECT PersonalInformation."Customer_ID"                         AS customer_id,
                                (EXTRACT(EPOCH FROM (SELECT * FROM analysformationdate)) -
                                 EXTRACT(EPOCH FROM max("Transaction_DateTime"))) / 86400 AS customer_inactive_period
                         FROM PersonalInformation
                                  JOIN Cards ON PersonalInformation."Customer_ID" = Cards."Customer_ID"
                                  JOIN Transactions ON Cards."Customer_Card_ID" = Transactions."Customer_Card_ID"
                         GROUP BY PersonalInformation."Customer_ID"
                         ORDER BY customer_inactive_period),
-- Расчитываем коэффициент оттока для каждого клиента
     Churn_Rate AS (SELECT Inactive_Period.customer_id,
                           Inactive_Period.customer_inactive_period /
                           Frequency.customer_frequency::numeric AS customer_churn_rate
                    FROM Inactive_Period
                             JOIN Frequency ON Inactive_Period.customer_id = Frequency.customer_id),
-- Сегментируем клиентов на основе вероятности оттока
     Churn_Segment AS (SELECT customer_id,
                              customer_churn_rate,
                              CASE
                                  WHEN customer_churn_rate < 2 THEN 'Low'
                                  WHEN customer_churn_rate >= 2 AND customer_churn_rate < 5 THEN 'Medium'
                                  ELSE 'High'
                                  END AS customer_churn_segment
                       FROM Churn_Rate),
-- Присваиваем номер сегмента клиенту на основе всех предыдущих сегментаций
     N_Segment AS (SELECT AC.customer_id AS customer_id,
                          S.Segment      AS customer_segment
                   FROM Average_Check_Segment AC
                            JOIN Frequency_Segment F ON AC.customer_id = F.customer_id
                            JOIN Churn_Segment CS ON AC.customer_id = CS.customer_id
                            JOIN Segments S ON
                       AC.customer_average_check_segment = S.average_check
                           AND F.customer_frequency_segment = S.frequency_of_purchases
                           AND CS.customer_churn_segment = S.churn_probability),
-- Определяем основной магазин для каждого клиента
     Transactions_Plus AS (SELECT cards."Customer_ID",
                                  cards."Customer_Card_ID",
                                  transactions."Transaction_ID",
                                  transactions."Transaction_Summ",
                                  transactions."Transaction_DateTime",
                                  transactions."Transaction_Store_ID"
                           FROM transactions
                                    JOIN cards ON cards."Customer_Card_ID" = transactions."Customer_Card_ID"),
     Primary_Store AS (SELECT "Customer_ID"          AS customer_id,
                              "Transaction_Store_ID" AS customer_primary_store
                       FROM (SELECT "Customer_ID",
                                    "Transaction_Store_ID",
                                    ROW_NUMBER()
                                    OVER (PARTITION BY "Customer_ID" ORDER BY "Transaction_DateTime" DESC) AS rn
                             FROM Transactions_Plus) sub
                       WHERE rn <= 1)
SELECT AC.customer_id                    AS Customer_ID,                    -- Идентификатор клиента
       AC.customer_average_check         AS Customer_Average_Check,         -- Значение среднего чека
       AC.customer_average_check_segment AS Customer_Average_Check_Segment, -- Сегмент по среднему чеку
       F.customer_frequency              AS Customer_Frequency,             -- Значение частоты транзакций
       F.customer_frequency_segment      AS Customer_Frequency_Segment,     -- Сегмент по частоте транзакций
       IP.Customer_Inactive_Period       AS Customer_Inactive_Period,       -- Количество дней после предыдущей транзакции
       CS.customer_churn_rate            AS Customer_Churn_Rate,            -- Коэффициент оттока
       CS.customer_churn_segment         AS Customer_Churn_Segment,         -- Сегмент по коэффициенту оттока
       NS.customer_segment               AS Customer_Segment,               -- Номер сегмента
       PS.customer_primary_store         AS Customer_Primary_Store          -- Идентификатор основного магазина
FROM Average_Check_Segment AS AC
         JOIN Frequency_Segment AS F ON AC.customer_id = F.customer_id
         JOIN Inactive_Period AS IP ON AC.customer_id = IP.customer_id
         JOIN Churn_Segment AS CS ON AC.customer_id = CS.customer_id
         JOIN N_Segment AS NS ON AC.customer_id = NS.customer_id
         JOIN Primary_Store AS PS ON AC.customer_id = PS.customer_id;

-- Тестовые запросы для материализованного представления Customers
SELECT *
FROM Customers;

SELECT *
FROM Customers
WHERE Customer_PRimary_Store = 1;

SELECT *
FROM Customers
WHERE Customer_Churn_Segment = 'Medium';

SELECT *
FROM Customers
WHERE Customer_Frequency > 120;

SELECT *
FROM Customers
WHERE Customer_Inactive_Period < 130
  AND Customer_Churn_Segment = 'Medium';

SELECT *
FROM Customers
WHERE Customer_Frequency_Segment = 'Occasionally'
ORDER BY customer_id DESC;
