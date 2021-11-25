SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET AUTOCOMMIT = 0;
START TRANSACTION;
SET time_zone = "+00:00";

CREATE DATABASE ticketsales;

USE ticketsales;

-- ticketsale table
-- -------------------------------------
CREATE TABLE ticketsale (
	ticket_id INT NOT NULL,
    trans_date DATE NOT NULL,
    event_id INT NOT NULL,
    event_name VARCHAR(50) NOT NULL,
    event_date DATE NOT NULL,
    event_type VARCHAR(10) NOT NULL,
    event_city VARCHAR(20) NOT NULL,
    customer_id INT NOT NULL,
    price DECIMAL NOT NULL,
    num_tickets INT NOT NULL
	) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- -------------------------------------
INSERT INTO ticketsale (ticket_id ,
    trans_date ,
    event_id ,
    event_name ,
    event_date ,
    event_type ,
    event_city ,
    customer_id,
    price,
    num_tickets )
	VALUES (1,2020-08-01,100,'The North American International Auto Show',2020-09-01,'Exhibition','Michigan',123,35.00,3),
		   (2,2020-08-03,101,'Carlisle Ford Nationals',2020-09-30,'Exhibition','Carlisle',151,43.00,1),
		   (3,2020-08-03,102,'Washington Spirits vs Sky Blue FC',2020-08-30,'Sports','Washington DC',223,59.34,5),
           (4,2020-08-05,103,'Christmas Spectacular',2020-10-05,'Theater','New York',223,89.95,2),
		   (5,2020-08-05,100,'The North American International Auto Show',2020-09-01,'Exhibition','Michigan',126,35.00,1),
		   (6,2020-08-05,103,'Christmas Spectacular',2020-10-05,'Theater','New York',1024,89.95,3)
           ;




SHOW VARIABLES LIKE "secure_file_priv";
show variables like "local_infile";
set global local_infile = 1;
mysql.server stop;

LOAD DATA INFILE '/Users/quanganhpham/Library/Application Support/MySQL/Workbench/data/third_party_sales_1.csv' 
INTO TABLE ticketsale 
FIELDS TERMINATED BY ',' 
;