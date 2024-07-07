-- Создание роли администратора
CREATE ROLE Administrator SUPERUSER CREATEDB CREATEROLE LOGIN REPLICATION BYPASSRLS;

-- Включаем все привелегии. "RetailAnalytics_v1" - заменить на имя вашей БД
GRANT ALL PRIVILEGES ON DATABASE "RetailAnalytics_v1" TO Administrator;

-- pg_signal_backend - встроенная функция в PostgreSQL, которая позволяет отправлять сигналы процессу PostgreSQL, идентифицированному по pid.
-- pg_execute_server_program - встроенная функция в PostgreSQL, которая позволяет выполнять внешние программы и скрипты
GRANT pg_signal_backend, pg_execute_server_program TO Administrator;


-- Создание роли посетителя
CREATE ROLE Visitor LOGIN;

-- Включаем необходимые привелегии для просмотра. "RetailAnalytics_v1" - заменить на имя вашей БД
GRANT CONNECT ON DATABASE "RetailAnalytics_v1" TO Visitor;
GRANT USAGE ON SCHEMA "retail21" TO Visitor;
GRANT SELECT ON ALL TABLES IN SCHEMA "retail21" TO Visitor;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "retail21" TO Visitor;


-- Проверим текущие права в сравнении с пользователем postgres
SELECT * FROM pg_catalog.pg_roles
WHERE rolname = 'administrator' OR rolname = 'visitor' OR rolname = 'postgres';

-- Проверка работы

-- Перключаемся на пользователя Visitor
SET ROLE Visitor;
-- Проверим доступ на чтение любой из таблиц
SELECT * FROM cards;
-- Попробуем создать таблицу и должны получить ошибку, что недостачно прав: "ERROR:  permission denied for schema retail21"
CREATE TABLE IF NOT EXISTS VisitorTest
(
    "ID" BIGINT PRIMARY KEY
);

-- Перключаемся на пользователя Administrator
SET ROLE Administrator;
-- Проверим доступ на чтение любой из таблиц
SELECT * FROM cards;
-- Попробуем создать таблицу
CREATE TABLE IF NOT EXISTS AdministratorTest
(
    "ID" BIGINT PRIMARY KEY
);
-- Удалим созданную таблицу, чтобы убедится в наличии полномочий
DROP TABLE IF EXISTS AdministratorTest CASCADE;

-- Переключимся на системного пользователя (или любого иного с нужным набором полномочий для дальнейшей работы)
SET ROLE postgres;

-- Удаление ролей
--DROP OWNED BY Administrator;
--DROP ROLE Administrator;
--DROP OWNED BY Visitor;
--DROP ROLE Visitor;