CREATE DATABASE podemosprograsar;

CREATE TABLE fact_weather(
date date not null, 
weatherid int not null, 
cityid int not null, 
temp float not null, 
feelslike float not null, 
tempmin float not null, 
tempmax float not null, 
pressure int not null, 
humidity int not null,
created_at timestamp not null, 
deleted_at timestamp); 


CREATE TABLE dim_weather(
weatherid serial,
weather varchar(30) not null, 
created_at timestamp not null, 
deleted_at timestamp, 
updated_at timestamp); 


CREATE TABLE dim_city(
cityid serial,
city varchar(30) not null,
countryId int not null,
created_at timestamp not null, 
deleted_at timestamp, 
updated_at timestamp); 


CREATE TABLE dim_country(
countryId serial,
country varchar(30) not null,
created_at timestamp not null, 
deleted_at timestamp, 
updated_at timestamp); 






insert into dim_country(country, created_at) values ('US', now());
insert into dim_country(country, created_at) values ('MX', now());
insert into dim_country(country, created_at) values ('GB', now());
	
DO $$
DECLARE
    var_country INT;
BEGIN
    -- Asignar el valor de una consulta a la variable
    SELECT countryId INTO var_country FROM dim_country where country= 'US';
   
    insert into dim_city(city,countryId, created_at ) values ('New York', var_country , now());	
END $$;

DO $$
DECLARE
    var_country INT;
BEGIN
    -- Asignar el valor de una consulta a la variable
    SELECT countryId INTO var_country FROM dim_country where country= 'MX';
   
    insert into dim_city(city,countryId, created_at ) values ('Mexico City', var_country , now());	
END $$;

DO $$
DECLARE
    var_country INT;
BEGIN
    -- Asignar el valor de una consulta a la variable
    SELECT countryId INTO var_country FROM dim_country where country= 'GB';
   
    insert into dim_city(city,countryId, created_at ) values ('London', var_country , now());	
END $$;