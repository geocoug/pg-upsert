drop table if exists public.genres cascade;
create table public.genres (
    genre varchar(100) primary key,
    description varchar not null
);

drop table if exists public.publishers cascade;
create table public.publishers (
    publisher_id varchar(100) primary key,
    publisher_name varchar(200) null
);

drop table if exists public.books cascade;
create table public.books (
    book_id varchar(100) primary key,
    book_title varchar(200) not null,
    genre varchar(100) not null,
    publisher_id varchar(100) null,
    notes text,
    foreign key (genre) references public.genres(genre)
        on update cascade
        on delete restrict,
    foreign key (publisher_id) references public.publishers(publisher_id)
        on update cascade
        on delete restrict
);

drop table if exists public.authors cascade;
create table public.authors (
    author_id varchar(100) primary key,
    first_name varchar(100) not null,
    last_name varchar(100) not null,
    constraint chk_authors check (first_name <> last_name),
    constraint chk_authors_first_name check (first_name ~ '^[a-zA-Z]+$'),
    constraint chk_authors_last_name check (last_name ~ '^[a-zA-Z]+$')
);

drop table if exists public.book_authors cascade;
create table public.book_authors (
    book_id varchar(100) not null,
    author_id varchar(100) not null,
    foreign key (author_id) references public.authors(author_id)
        on update cascade
        on delete restrict,
    foreign key (book_id) references public.books(book_id)
        on update cascade
        on delete restrict,
    constraint pk_book_authors primary key (book_id, author_id)
);

-- Create staging tables that mimic base tables.
-- Note: staging tables have the same columns as base tables but no PK, FK, NOT NULL, or Check constraints.
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
    book_id varchar(100),
    book_title varchar(200),
    genre varchar(100),
    publisher_id varchar(100),
    notes text
);

drop table if exists staging.authors cascade;
create table staging.authors (
    author_id varchar(100),
    first_name varchar(100),
    last_name varchar(100)
);

drop table if exists staging.book_authors cascade;
create table staging.book_authors (
    book_id varchar(100),
    author_id varchar(100)
);

-- Insert data into staging tables.
insert into staging.genres (genre, description) values
    ('Fiction', 'Literary works that are imaginary, not based on real events or people'),
    ('Non-Fiction', 'Literary works based on real events, people, and facts');

insert into staging.publishers (publisher_id, publisher_name) values
    ('P001', 'Publisher 1'),
    ('P002', null);

insert into staging.authors (author_id, first_name, last_name) values
    ('JDoe', 'John', 'Doe'),
    ('JSmith', 'Jane', 'Smith'),
    ('JTrent', 'Joe', 'Trent');

insert into staging.books (book_id, book_title, genre, publisher_id, notes) values
    ('B001', 'The Great Novel', 'Fiction', 'P001', 'An epic tale of love and loss'),
    ('B002', 'Not Another Great Novel', 'Non-Fiction', null, 'A comprehensive guide to writing a great novel');

insert into staging.book_authors (book_id, author_id) values
    ('B001', 'JDoe'),
    ('B001', 'JTrent'),
    ('B002', 'JSmith');
