
DROP TABLE IF EXISTS "bank.Customers";


create Table bank.Customers (
  customer_id serial primary key not null,
  first_name varchar(50) ,
  last_name varchar(50),
  address varchar(100),
  phone varchar(50) not null,
  email varchar(50) not null,
  dateofbirth date 
)