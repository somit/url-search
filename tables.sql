create database urldb

create table url_desc(
     url_id INT NOT NULL AUTO_INCREMENT,
     url VARCHAR(3000) NOT NULL,
     uid INT NOT NULL,
     description VARCHAR(3000) NOT NULL,
     created timestamp default CURRENT_TIMESTAMP NOT NULL,
     PRIMARY KEY (url_id)
);

