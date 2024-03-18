CREATE DATABASE intus_films;
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(); 

CREATE OR REPLACE TABLE film (
film_id NUMBER,
title STRING,
description STRING,
release_year DATE,
language_id NUMBER,
original_language_id NUMBER,
rental_duration NUMBER,
rental_date NUMBER,
length NUMBER,
replacement_cost NUMBER,
rating STRING,
special_features STRING,
last_update DATE
);

CREATE TABLE language (
language_id NUMBER,
name STRING,
last_update DATE
);

CREATE TABLE inventory (
inventory_id NUMBER,
film_id NUMBER,
store_id NUMBER,
last_update DATE
);

CREATE TABLE film_actor (
actor_id NUMBER,
film_id NUMBER,
last_update DATE
);

CREATE TABLE actor (
actor_id NUMBER,
first_name STRING,
last_name STRING,
last_update DATE
);

CREATE TABLE film_category (
film_id NUMBER,
category_id NUMBER,
last_update DATE
);

CREATE TABLE category (
category_id NUMBER,
name STRING,
last_update DATE
);
