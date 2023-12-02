-- SQL-команды для создания таблиц
CREATE TABLE employees
(
    employee_id int PRIMARY KEY NOT NULL,
    first_name varchar(20) NOT NULL,
    last_name varchar(20) NOT NULL,
    title text,
    birth_date date,
    notes text
);
CREATE TABLE customers
(
    customer_id varchar(5) PRIMARY KEY NOT NULL,
    company_name varchar(50) NOT NULL,
    contact_name varchar(40) NOT NULL
);

CREATE TABLE orders
(
    order_id int PRIMARY KEY NOT NULL,
    customer_id varchar(5) REFERENCES customers(customer_id) NOT NULL,
    employee_id int REFERENCES employees(employee_id) NOT NULL,
    order_date date NOT NULL,
    ship_city varchar(30) NOT NULL
);

SELECT * FROM customers
SELECT * FROM orders
SELECT * FROM employees