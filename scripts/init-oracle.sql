-- Oracle Database Initialization Script
-- 
-- Purpose: Initialize Oracle database for Arni integration testing
-- Database: FREE (Oracle Free database instance)
-- User: system
-- 
-- This script runs automatically when the Oracle container starts for the first time.
-- It creates the test schema, tables, and populates them with sample data.
--
-- Usage: This file is mounted in compose.yml and executed by Oracle's
-- startup scripts mechanism at /opt/oracle/scripts/startup/
--
-- Note: Oracle syntax differs from PostgreSQL/MySQL/SQL Server
-- - Use SEQUENCE + TRIGGER for auto-increment
-- - Use VARCHAR2 instead of VARCHAR
-- - Use NUMBER for numeric types

-- Drop table if exists (idempotent)
begin
   execute immediate 'DROP TABLE users CASCADE CONSTRAINTS';
exception
   when others then
      if sqlcode != -942 then
         raise;
      end if;
end;
/

-- Drop sequence if exists (idempotent)
begin
   execute immediate 'DROP SEQUENCE users_seq';
exception
   when others then
      if sqlcode != -2289 then
         raise;
      end if;
end;
/

-- Create users table
create table users (
   id         number primary key,
   name       varchar2(100) not null,
   email      varchar2(255) unique not null,
   active     number(1) default 1,
   created_at timestamp default current_timestamp
);

-- Create sequence for auto-increment
create sequence users_seq start with 1 increment by 1;

-- Create trigger for auto-increment
create or replace trigger users_auto_increment before
   insert on users
   for each row
begin
   if :new.id is null then
      :new.id := users_seq.nextval;
   end if;
end;
/

-- Insert sample data
insert into users (
   name,
   email,
   active,
   created_at
) values ( 'Alice Johnson',
           'alice@example.com',
           1,
           current_timestamp );
insert into users (
   name,
   email,
   active,
   created_at
) values ( 'Bob Smith',
           'bob@example.com',
           1,
           current_timestamp );
insert into users (
   name,
   email,
   active,
   created_at
) values ( 'Charlie Brown',
           'charlie@example.com',
           0,
           current_timestamp );
insert into users (
   name,
   email,
   active,
   created_at
) values ( 'Diana Prince',
           'diana@example.com',
           1,
           current_timestamp );
insert into users (
   name,
   email,
   active,
   created_at
) values ( 'Eve Adams',
           'eve@example.com',
           1,
           current_timestamp );

-- Commit the data
commit;

-- Create indexes for performance
create index idx_users_email on
   users (
      email
   );
create index idx_users_active on
   users (
      active
   );