-- CHALLENGE 1 -----------------------------------------
-- 1. Rank films by their length and create an output table that includes the title, length, and rank columns only. 
-- Filter out any rows with null or zero values in the length column.
SELECT title, length, DENSE_RANK() OVER(ORDER BY length DESC) AS 'Rank'
 FROM film
  WHERE IFNULL(length,0) > 0;

-- 2. Rank films by length within the rating category and create an output table that includes the title, length, rating and rank columns only. 
-- Filter out any rows with null or zero values in the length column.
SELECT title, length, rating , DENSE_RANK() OVER(PARTITION BY rating ORDER BY length DESC) AS "Ranks"
 FROM film
  WHERE IFNULL(length,0) > 0;
 
-- 3. Produce a list that shows for each film in the Sakila database, the actor or actress who has acted in the greatest number of films, 
-- as well as the total number of films in which they have acted. Hint: Use temporary tables, CTEs, or Views when appropiate to simplify your queries.

-- Parte #1: VIEW con número de films en los que ha participado cada actor
DROP VIEW sakila.nr_films_by_actor;
CREATE VIEW sakila.nr_films_by_actor AS 
SELECT a.actor_id, CONCAT(a.first_name,' ',a.last_name) AS nombre_actor, COUNT(fa.film_id) AS num_films
 FROM actor a INNER JOIN film_actor fa ON (a.actor_id=fa.actor_id)
  GROUP BY a.actor_id;
SELECT * 
 FROM nr_films_by_actor;
 
-- Parte #2: CTE con todos los actores que participan en cada film, 
-- y usando WINDOW function para calcular el que ha participado en más films.
WITH cte_fa AS (
  SELECT 
   f.film_id,
   f.title,
   nr.actor_id,
   nr.nombre_actor,
   nr.num_films,
   MAX(nr.num_films) OVER(PARTITION BY f.film_id) AS max_films
FROM film f 
 INNER JOIN film_actor fa ON (f.film_id=fa.film_id)
 INNER JOIN nr_films_by_actor nr ON (fa.actor_id=nr.actor_id)
 ORDER BY f.film_id, nr.num_films DESC)
SELECT film_id, title, actor_id, nombre_actor, num_films
 FROM cte_fa
  WHERE num_films=max_films;


 
-- CHALLENGE 2 -----------------------------------------
-- Step 1. Retrieve the number of monthly active customers, i.e., the number of unique customers who rented a movie in each month.
CREATE OR REPLACE VIEW customer_activity AS
SELECT customer_id, 
       CONVERT(rental_date, DATE) AS Activity_date,
       DATE_FORMAT(CONVERT(rental_date,DATE), '%M') AS Activity_Month,
       DATE_FORMAT(CONVERT(rental_date,DATE), '%m') AS Activity_Month_number,
       DATE_FORMAT(CONVERT(rental_date,DATE), '%Y') AS Activity_year
FROM rental;
SELECT * 
 FROM customer_activity;

SELECT 
   Activity_year, 
   Activity_Month, 
   Activity_Month_number, 
   COUNT(DISTINCT customer_id) AS Active_customers
FROM customer_activity
GROUP BY Activity_year, Activity_Month, Activity_Month_number
ORDER BY Activity_year ASC, Activity_Month_number ASC;

-- Step 2. Retrieve the number of active users in the previous month.
DROP VIEW monthly_active_customers;
CREATE VIEW monthly_active_customers AS
 SELECT 
   Activity_year, 
   Activity_Month, 
   Activity_Month_number, 
   COUNT(DISTINCT customer_id) AS Active_customers
FROM customer_activity
GROUP BY Activity_year, Activity_Month, Activity_Month_number
ORDER BY Activity_year ASC, Activity_Month_number ASC;

SELECT 
   Activity_year,
   Activity_month,
   Active_customers,
   LAG(Active_customers,1) OVER(ORDER BY Activity_year, Activity_Month_number) AS Last_month
FROM monthly_active_customers;

-- Step 3. Calculate the percentage change in the number of active customers between the current and previous month.
WITH cte_view AS (
SELECT 
   Activity_year,
   Activity_month,
   Active_customers,
   LAG(Active_customers,1) OVER(ORDER BY Activity_year, Activity_Month_number) AS Last_month
FROM monthly_active_customers)
SELECT 
   Activity_year, 
   Activity_month, 
   Active_customers,
   Last_month, 
   ROUND(((Active_customers - Last_month) / Last_month) * 100, 2) AS '% Difference' 
FROM cte_view;

-- Step 4. Calculate the number of retained customers every month, i.e., customers who rented movies in the current and previous months.

-- Calcular año/mes de rental, esta vez incluyendo el rental_id además de customer_id, para ver el número de rentals por cliente.
CREATE OR REPLACE VIEW rental_activity AS
SELECT rental_id,
	   customer_id, 
       CONVERT(rental_date, DATE) AS Activity_date,
       DATE_FORMAT(CONVERT(rental_date,DATE), '%M') AS Activity_Month,
       DATE_FORMAT(CONVERT(rental_date,DATE), '%m') AS Activity_Month_number,
       DATE_FORMAT(CONVERT(rental_date,DATE), '%Y') AS Activity_year       
FROM rental;
SELECT * 
 FROM rental_activity;

-- Calcular el número de rentals por cliente.
DROP VIEW monthly_rental_activity;
CREATE VIEW monthly_rental_activity AS 
SELECT 
   customer_id,
   Activity_year, 
   Activity_Month, 
   Activity_Month_number,
   COUNT(rental_id) AS num_rentals
FROM rental_activity
GROUP BY customer_id, Activity_year, Activity_Month, Activity_Month_number
ORDER BY customer_id ASC, Activity_year ASC, Activity_Month_number ASC;
SELECT * 
 FROM monthly_rental_activity;

-- Calcular el número de rentals del mes anterior, por cliente. 
-- DROP VIEW monthly_rental_lag;
-- CREATE VIEW monthly_rental_lag AS 
-- SELECT 
--    customer_id,
--    Activity_year,
--    Activity_month,
--    Activity_month_number,
--    num_rentals,
--    LAG(num_rentals,1) OVER(PARTITION BY customer_id ORDER BY customer_id, Activity_year, Activity_Month_number) AS last_month
-- 	FROM monthly_rental_activity
-- 	ORDER BY customer_id ASC;
-- SELECT * 
-- 	FROM monthly_rental_lag;

-- SELECT 
--  Activity_year,
--  Activity_month,
--  COUNT(last_month) AS Customer_Retention
--  FROM monthly_rental_lag
--   GROUP BY Activity_year, Activity_month;   
      
SELECT todos.yr AS Activity_year, todos.mes AS Activity_Month, COUNT(todos.cust) AS Clientes_retenidos
 FROM (
SELECT t1.customer_id AS cust, t1.Activity_year AS yr, t1.Activity_month AS mes, t1.Activity_month_number AS mes_nr, t1.num_rentals AS cuantos, t2.customer_id AS cid, t2.Activity_month, t2.Activity_month_number, t2.num_rentals
	FROM monthly_rental_activity t1
     INNER JOIN monthly_rental_activity t2 ON (t1.Activity_year=t2.Activity_year AND t1.Activity_month_number=t2.Activity_month_number + 1 AND t1.customer_id=t2.customer_id)
     -- WHERE t1.customer_id in (1,213,216)
UNION
SELECT t1.customer_id AS cust, t1.Activity_year AS yr, t1.Activity_month AS mes, t1.Activity_month_number AS mes_nr, t1.num_rentals AS cuantos, t2.customer_id AS cid, t2.Activity_month, t2.Activity_month_number, t2.num_rentals
	FROM monthly_rental_activity t1
     INNER JOIN monthly_rental_activity t2 ON (t1.Activity_year=2006 AND t2.Activity_year=2005 AND t1.Activity_month_number=02 AND t2.Activity_month_number=08 AND t1.customer_id=t2.customer_id)
     -- WHERE t1.customer_id in (1,213,216)
     ) AS todos
GROUP BY yr, mes_nr, mes
ORDER BY yr, mes_nr;
      
     
  
        

