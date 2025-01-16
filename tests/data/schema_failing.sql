/*------------------------------------------------------------------------------------------------
Author: Caleb Grant
Date: 2025-01-16
URL: https://github.com/geocoug/pg-upsert
DBMS: PostgreSQL
Description:
    This script creates a simple database schema for a book library system
    under the public schema. Another schema called staging is created that holds
    the same tables, but with out any constraints (PK, FK, NOT NULL, or Check constraints).

    Sample data are inserted into the staging tables with the intention
    of UPSERTING them into the base tables using the pg-upsert Python library.
    The sample data includes valid and invalid records to test the UPSERT functionality.

    The schema includes the following tables:
        - genres: Contains information about book genres.
        - publishers: Contains information about book publishers.
        - books: Contains information about books, including title, genre, publisher, and notes.
        - authors: Contains information about authors, including first name, last name, and author ID.
        - book_authors: Contains a many-to-many relationship between books and authors.

    The schema also includes the following functions and triggers:
        - currentuser(): A function that returns the current database user.
        - set_rev_time(): A function that sets the rev_time and rev_user columns on insert or update.
        - revtime: A trigger that calls the set_rev_time function before insert or update on base tables.
------------------------------------------------------------------------------------------------*/


/*---------------------------------------------
    Function to get the current database user.
---------------------------------------------*/
CREATE OR REPLACE FUNCTION public.currentuser()
   RETURNS text
   LANGUAGE plpgsql
   IMMUTABLE STRICT
AS $function$
BEGIN
  RETURN current_user;
END;
$function$;

/*---------------------------------------------
    Function to set the rev_time and rev_user
    columns on insert or update.
---------------------------------------------*/
CREATE OR REPLACE FUNCTION public.set_rev_time()
   RETURNS trigger
   LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.rev_time := current_timestamp;
    NEW.rev_user := current_user;
  return NEW;
END;
$function$;


/*---------------------------------------------
    Create base tables in the public schema
---------------------------------------------*/
drop table if exists public.genres cascade;
create table public.genres (
    genre varchar(100) primary key,
    description varchar not null,
	rev_time timestamp DEFAULT now() NULL,
	rev_user varchar(25) DEFAULT currentuser() NULL
);
create trigger revtime before insert or update
on public.genres for each row execute function set_rev_time();


drop table if exists public.publishers cascade;
create table public.publishers (
    publisher_id varchar(100) primary key,
    publisher_name varchar(200) null,
	rev_time timestamp DEFAULT now() NULL,
	rev_user varchar(25) DEFAULT currentuser() NULL
);
create trigger revtime before insert or update
on public.publishers for each row execute function set_rev_time();


drop table if exists public.books cascade;
create table public.books (
    book_id varchar(40) primary key,
    book_title varchar(200) not null,
    genre varchar(100) not null,
    publisher_id varchar(100) null,
    notes text,
    book_alias serial not null,
	rev_time timestamp DEFAULT now() NULL,
	rev_user varchar(25) DEFAULT currentuser() NULL,
    foreign key (genre) references public.genres(genre)
        on update cascade
        on delete restrict,
    foreign key (publisher_id) references public.publishers(publisher_id)
        on update cascade
        on delete restrict
);
create trigger revtime before insert or update
on public.books for each row execute function set_rev_time();


drop table if exists public.authors cascade;
create table public.authors (
    author_id varchar(60) primary key,
    first_name varchar(60) not null,
    last_name varchar(60) not null,
    email varchar(100) null,
	rev_time timestamp DEFAULT now() NULL,
	rev_user varchar(25) DEFAULT currentuser() NULL,
    constraint chk_authors_first_name check (first_name ~ '^[a-zA-Z]+$'),
    constraint chk_authors_last_name check (last_name ~ '^[a-zA-Z]+$'),
    constraint chk_authors_email check (email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
);
create trigger revtime before insert or update
on public.authors for each row execute function set_rev_time();


drop table if exists public.book_authors cascade;
create table public.book_authors (
    book_id varchar(40) not null,
    author_id varchar(60) not null,
	rev_time timestamp DEFAULT now() NULL,
	rev_user varchar(25) DEFAULT currentuser() NULL,
    foreign key (author_id) references public.authors(author_id)
        on update cascade
        on delete restrict,
    foreign key (book_id) references public.books(book_id)
        on update cascade
        on delete restrict,
    constraint pk_book_authors primary key (book_id, author_id)
);
create trigger revtime before insert or update
on public.book_authors for each row execute function set_rev_time();


/*---------------------------------------------
    Create staging tables that mimic base tables.

    NOTE: These tables do not have any constraints
          (PK, FK, NOT NULL, or Check constraints).
          In addition, they do not have the rev_time
          and rev_user columns as those are automatically
          populated by triggers in the base tables.
---------------------------------------------*/
create schema if not exists staging;

drop table if exists staging.genres cascade;
create table staging.genres (
    genre varchar(100),
    description varchar
);

drop table if exists staging.publishers cascade;
create table staging.publishers (
    publisher_id varchar(100),
    publisher_name varchar(200)
);

drop table if exists staging.books cascade;
create table staging.books (
    book_id varchar(40),
    book_title varchar(200),
    genre varchar(100),
    publisher_id varchar(100),
    notes text,
    book_alias serial
);

drop table if exists staging.authors cascade;
create table staging.authors (
    author_id varchar(60),
    first_name varchar(60),
    last_name varchar(60),
    email varchar(100)
);

drop table if exists staging.book_authors cascade;
create table staging.book_authors (
    book_id varchar(40),
    author_id varchar(60)
);


/*---------------------------------------------
    Insert sample data into staging tables.
---------------------------------------------*/
insert into staging.genres (genre, description) values
    ('Fiction', 'Literary works that are imaginary, not based on real events or people'),
    ('Non-Fiction', 'Literary works based on real events, people, and facts'),
    ('Sci-Fi', 'Literary works that explore futuristic concepts and technologies'),
    ('Fantasy', 'Literary works that involve magic, mythical creatures, and supernatural elements'),
    ('Mystery', 'Literary works that involve solving a crime or puzzle'),
    ('Romance', 'Literary works that focus on romantic relationships'),
    ('Horror', 'Literary works that are intended to scare, disgust, or startle the reader'),
    ('Thriller', 'Literary works that are fast-paced, suspenseful, and thrilling'), -- This row will fail due to duplicate genre
    ('Thriller', 'Literary works that are fast-paced, suspenseful, and thrillling'), -- This row will fail due to duplicate genre
    ('Biography', 'Literary works that tell the story of a person''s life written by someone else'),
    ('Autobiography', 'Literary works that tell the story of a person''s life written by the person themselves'),
    ('Self-Help', 'Literary works that provide advice and guidance on personal growth and self-improvement'),
    ('Cookbook', 'Literary works that provide recipes and cooking instructions'),
    ('Travel', 'Literary works that describe places, cultures, and experiences from the author''s perspective'),
    ('History', 'Literary works that describe past events, people, and societies'),
    ('Science', 'Literary works that explain scientific concepts, theories, and discoveries'),
    (null, 'Literary works that explore artistic movements, styles, and techniques'), -- This row will fail due to NULL genre
    ('Poetry', 'Literary works that use rhythmic and expressive language to evoke emotions and imagery'),
    ('Drama', null), -- This row will fail due to NULL description
    ('Comedy', 'Literary works that are intended to be humorous and entertaining');

insert into staging.publishers (publisher_id, publisher_name) values
    ('P001', 'Great Publishing House'),
    ('P002', 'Another Publishing Company'),
    ('P003', 'Bestseller Books'),
    (null, 'Famous Authors Press'), -- This row will fail due to NULL publisher_id
    ('P005', 'New Horizon Publishers'),
    ('P006', 'Classic Literature Co.'),
    ('P007', 'Modern Fiction Group'),
    ('P008', 'Sci-Fi Fantasy Inc.'), -- This row will fail due to duplicate publisher_id
    ('P008', 'Sci-Fi Fantasy Inc'), -- This row will fail due to duplicate publisher_id
    ('P009', 'Mystery Thriller Books'),
    ('P010', 'Romantic Novels Ltd.'),
    ('P011', 'Horror Stories Publishing'),
    ('P012', 'Biography Autobiography House'),
    ('P013', 'Self-Help Guides Inc.'),
    ('P014', 'Culinary Creations Press'),
    ('P015', null), -- This row will fail due to NULL publisher_name
    ('P016', 'Historical Society Books'),
    ('P017', 'Science Explained Co.'),
    ('P018', 'Artistic Expressions Ltd.'),
    ('P019', 'Poetry Lovers Press'),
    ('P020', 'Dramatic Works Publishing'),
    ('P021', 'Comedy Central Books');

insert into staging.authors (author_id, first_name, last_name, email)
values
    ('JDoe', 'John', 'Doe', 'john.doe@email.com'), -- This row will fail due to duplicate author_id
    ('JDoe', 'John', 'Doe', 'johndoe@email.com'), -- This row will fail due to duplicate author_id
    ('AAdams', 'Alice', 'Adams', 'alice.adams@email.com'),
    ('BBrown', 'Bob', 'Brown', null), -- This row will fail due to duplicate author_id
    ('BBrown', 'Bob', 'Brown', null), -- This row will fail due to duplicate author_id
    ('CCooper', 'Cathy', 'Cooper', 'cathy_cooper2@email.com'),
    ('DDavis', 'David', 'Davis', 'ddavis@email.com'),
    ('EEvans', null, 'Evans', 'emilyevans@email.com'), -- This row will fail due to NULL first_name
    ('FFisher', 'Frank', 'Fisher', 'frankfisher@email.com'),
    ('GGarcia', 'George', null, 'georgegarcia@email.com'), -- This row will fail due to NULL last_name
    ('HHall', 'Helen', 'Hall', 'hhall@email.com'),
    ('IIngram', 'Isaac', 'Ingram', 'i_s_a_a_c@email.com'),
    ('MMike', 'M*', 'Mike', 'mikeandmike@email.com'), -- This row will fail due to check constraint on first_name
    ('1White', '1White', '1', 'mwhite@email.com'), -- This row will fail due to check constraint on first_name and last_name
    ('JJones', 'Jack', 'Jones', 'jack jones'), -- This row will fail due to check constraint on email
    ('KKing', 'Katie', 'King', 'katie_king@email.com'),
    (null, 'Mary', 'Moore', 'mmoore@email.com'), -- This row will fail due to NULL author_id
    ('LLee', 'Larry', 'Lee', 'llee@email.com');

insert into staging.books (book_id, book_title, genre, publisher_id, notes) values
    ('B001', 'The Great Novel', 'Fiction', 'P001', 'An epic tale of love and loss'),
    ('B002', 'Not Another Great Novel', 'Non-Fiction', null, 'A comprehensive guide to writing a great novel'),
    (null,   'Sci-Fi Adventures', 'Sci-Fi', 'P008', 'Explore the universe with these thrilling stories'), -- This row will fail due to NULL book_id
    ('B004', 'Fantasy Quest', 'Fantasy', 'P006', 'Embark on a magical journey to save the kingdom'),
    ('B005', 'Mystery Mansion', 'Mystery', 'P009', 'Solve the mystery of the haunted mansion'),
    ('B006', 'Romantic Escapades', 'Romance', 'P010', 'Fall in love with these romantic tales'),
    ('B007', 'Horror Stories', 'Horrorr', 'P011', 'Prepare to be scared with these chilling stories'), -- This row will fail due to FK constraint on genre (misspelled)
    ('B008', 'Thriller Frenzy', 'Thriller', 'P009', 'Fast-paced action and suspense await you'),
    ('B009', 'Biography of a Legend', 'Biography', 'P012', 'Discover the life of a legendary figure'), -- This row will fail due to PK constraint on book_id
    ('B009', 'Biography of a Legend 2', 'Biography', 'P012', 'Discover the life of a legendary figure'), -- This row will fail due to PK constraint on book_id
    ('B010', 'Autobiography of Me', 'Autobiography', 'P012', 'My life story in my own words'),
    ('B011', 'Self-Help Guide', 'Self-Help', 'P013', 'Improve your life with these practical tips'),
    ('B012', 'Culinary Creations', null, 'P014', 'Delicious recipes for every occasion'), -- This row will fail due to NULL genre
    ('B013', 'Travel Tales', 'Travel', 'P015', 'Explore the world through the eyes of the author'),
    ('B014', 'Historical Events', 'History', 'P016', 'Learn about key events that shaped history'),
    ('B015', 'Science Explained', 'Science', 'P017', 'Understand complex scientific concepts with ease'),
    ('B016', 'Poetry Collection', 'Poetry', 'P999', 'Experience the power of poetry in all its forms'), -- This row will fail due to FK constraint on publisher_id
    ('B017', null, 'Drama', 'P020', 'Plays that will captivate and entertain audiences'), -- This row will fail due to NULL book_title
    ('B018', 'Comedy Central', 'Comedy', 'P021', 'Laugh out loud with these hilarious stories');

insert into staging.book_authors (book_id, author_id) values
    ('B001', 'JDoe'),
    ('B002', 'JDoe'), -- This row will fail due to PK constraint on book_id & author_id
    ('B002', 'JDoe'), -- This row will fail due to PK constraint on book_id & author_id
    ('B004', 'AAdams'),
    (null,   'BBrown'), -- This row will fail due to NULL book_id
    ('B005', 'CCooper'),
    ('B006', null), -- This row will fail due to NULL author_id
    ('B007', 'DDavis'),
    ('B008', 'FFisher'),
    ('B009', 'GGarcia'),
    ('B010', 'HHalll'), -- This row will fail due to FK constraint on author_id (misspelled)
    ('B011', 'IIngram'),
    ('B012', 'KKing'),
    ('B013', 'LLee'),
    ('B999', 'JDoe'), -- This row will fail due to FK constraint on book_id
    ('B015', 'BBrown'),
    ('B016', 'JDoe'),
    ('B017', 'JDoe'),
    ('B018', 'JDoe');
