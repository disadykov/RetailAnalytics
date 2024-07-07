-- Создаем новую SCHEMA
DROP SCHEMA IF EXISTS retail21 CASCADE;
CREATE SCHEMA retail21;

-- Устанавливаем retail21 как схему по умолчанию
SET search_path TO retail21;

-- Создаем таблицу Персональные данные
DROP TABLE IF EXISTS PersonalInformation CASCADE;
CREATE TABLE IF NOT EXISTS PersonalInformation
(
    "Customer_ID"            BIGINT PRIMARY KEY,
    "Customer_Name"          VARCHAR NOT NULL CHECK ("Customer_Name" ~ '^[A-ZА-Я][a-zа-я\s\-]*$'),
    "Customer_Surname"       VARCHAR NOT NULL CHECK ("Customer_Surname" ~ '^[A-ZА-Я][a-zа-я\s\-]*$'),
    "Customer_Primary_Email" VARCHAR CHECK ("Customer_Primary_Email" ~ '^\w+@\w+\.\w+$'),
    "Customer_Primary_Phone" VARCHAR CHECK ("Customer_Primary_Phone" ~ '^\+7[0-9]{10}$')
);

-- Создаем таблицу Карты
DROP TABLE IF EXISTS Cards CASCADE;
CREATE TABLE IF NOT EXISTS Cards
(
    "Customer_Card_ID" BIGINT PRIMARY KEY,
    "Customer_ID"      BIGINT NOT NULL REFERENCES PersonalInformation ("Customer_ID")
);

-- Создаем таблицу Транзакции
DROP TABLE IF EXISTS Transactions CASCADE;
CREATE TABLE IF NOT EXISTS Transactions
(
    "Transaction_ID"       BIGINT PRIMARY KEY,
    "Customer_Card_ID"     BIGINT    NOT NULL, 
    "Transaction_Summ"     DECIMAL   NOT NULL,
    "Transaction_DateTime" TIMESTAMP NOT NULL,
    "Transaction_Store_ID" BIGINT    NOT NULL,
    FOREIGN KEY ("Customer_Card_ID") REFERENCES Cards ("Customer_Card_ID")
);

-- Создаем таблицу Группы SKU
DROP TABLE IF EXISTS SKUGroup CASCADE;
CREATE TABLE IF NOT EXISTS SKUGroup
(
    "Group_ID"   BIGINT PRIMARY KEY,
    "Group_Name" VARCHAR(255) NOT NULL
);

-- Создаем таблицу Товарная матрица
DROP TABLE IF EXISTS ProductGrid CASCADE;
CREATE TABLE IF NOT EXISTS ProductGrid
(
    "SKU_ID"   BIGINT PRIMARY KEY,
    "SKU_Name" VARCHAR(255) NOT NULL,
    "Group_ID" BIGINT       NOT NULL REFERENCES SKUGroup ("Group_ID")
);

-- Создаем таблицу Чеки
DROP TABLE IF EXISTS Checks CASCADE;
CREATE TABLE IF NOT EXISTS Checks
(
    "Transaction_ID" BIGINT,
    "SKU_ID"         BIGINT  NOT NULL,
    "SKU_Amount"     DECIMAL NOT NULL CHECK ("SKU_Amount" >= 0),
    "SKU_Summ"       DECIMAL NOT NULL CHECK ("SKU_Summ" >= 0),
    "SKU_Summ_Paid"  DECIMAL NOT NULL CHECK ("SKU_Summ_Paid" >= 0),
    "SKU_Discount"   DECIMAL NOT NULL CHECK ("SKU_Discount" >= 0),
    FOREIGN KEY ("Transaction_ID") REFERENCES Transactions ("Transaction_ID"),
    FOREIGN KEY ("SKU_ID") REFERENCES ProductGrid ("SKU_ID")
);

-- Создаем таблицу Торговые точки
DROP TABLE IF EXISTS Stores CASCADE;
CREATE TABLE IF NOT EXISTS Stores
(
    "Transaction_Store_ID" BIGINT,
    "SKU_ID"               BIGINT  NOT NULL REFERENCES ProductGrid ("SKU_ID"),
    "SKU_Purchase_Price"   DECIMAL NOT NULL CHECK ("SKU_Purchase_Price" >= 0),
    "SKU_Retail_Price"     DECIMAL NOT NULL CHECK ("SKU_Retail_Price" >= 0)
);

-- Создаем таблицу Дата формирования анализа
DROP TABLE IF EXISTS AnalysFormationDate CASCADE;
CREATE TABLE IF NOT EXISTS AnalysFormationDate
(
    Analysis_Formation TIMESTAMP NOT NULL
);

-- Удаляем существующие процедуры, если они существуют
DROP PROCEDURE IF EXISTS csv_import(varchar, varchar, varchar);
DROP PROCEDURE IF EXISTS csv_export(varchar, varchar, varchar);
DROP PROCEDURE IF EXISTS tsv_import(varchar, varchar);
DROP PROCEDURE IF EXISTS tsv_export(varchar, varchar);

-- Создаем процедуру для импорта данных из CSV файла
CREATE OR REPLACE PROCEDURE csv_import(table_name VARCHAR, file_path VARCHAR, sep VARCHAR) AS
$$
BEGIN
    EXECUTE FORMAT('COPY %I FROM %L DELIMITER %L CSV HEADER;', table_name, file_path, sep);
END;
$$ LANGUAGE plpgsql;

-- Создаем процедуру для экспорта данных в CSV файл
CREATE OR REPLACE PROCEDURE csv_export(table_name VARCHAR, file_path VARCHAR, sep VARCHAR) AS
$$
BEGIN
    EXECUTE FORMAT('COPY %I TO %L WITH DELIMITER %L CSV HEADER;', table_name, file_path, sep);
END;
$$ LANGUAGE plpgsql;

-- Создаем процедуру для импорта данных из TSV файла
CREATE OR REPLACE PROCEDURE tsv_import(table_name VARCHAR, file_path VARCHAR) AS
$$
BEGIN
    EXECUTE FORMAT('COPY %I FROM %L DELIMITER E''\t'';', table_name, file_path);
END;
$$ LANGUAGE plpgsql;

-- Создаем процедуру для экспорта данных в TSV файл
CREATE OR REPLACE PROCEDURE tsv_export(table_name VARCHAR, file_path VARCHAR) AS
$$
BEGIN
    EXECUTE FORMAT('COPY %I TO %L WITH DELIMITER E''\t'';', table_name, file_path);
END;
$$ LANGUAGE plpgsql;

-- Устанавливаем стиль отображения дат в формат ISO с указанием дня, месяца и года
SET DATESTYLE TO 'ISO, DMY';

-- Вызываем процедуры импорта данных из TSV файла для таблиц
-- Необходимо заменить file_path на свой путь до datasets/*.tsv файлов
-- Для директории datasets необходимо установить владельца/права для пользователя от которого работате в postgres
-- sudo chown postgres datasets
-- Также на маке рекоммедуется перенести datasets/ в /tmp директорию
-- cp -r datasets /tmp
CALL tsv_import('personalinformation', '/tmp/datasets/Personal_Data_Mini.tsv');
CALL tsv_import('cards', '/tmp/datasets/Cards_Mini.tsv');
CALL tsv_import('transactions', '/tmp/datasets/Transactions_Mini.tsv');
CALL tsv_import('skugroup', '/tmp/datasets/Groups_SKU_Mini.tsv');
CALL tsv_import('productgrid', '/tmp/datasets/SKU_Mini.tsv');
CALL tsv_import('checks', '/tmp/datasets/Checks_Mini.tsv');
CALL tsv_import('stores', '/tmp/datasets/Stores_Mini.tsv');
CALL tsv_import('analysformationdate', '/tmp/datasets/Date_Of_Analysis_Formation.tsv');
