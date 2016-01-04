
delete from mysql.user where user='fabric';
flush privileges;
CREATE USER 'fabric' identified by 'fabric';
GRANT ALL ON *.* TO 'fabric';
flush privileges;

