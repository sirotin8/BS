
delete from mysql.user where user='fabric';
flush privileges;
CREATE USER 'fabric' IDENTIFIED BY 'fabric';
GRANT ALL ON *.* TO 'fabric';
flush privileges;

