## JKISS Cassandra JDBC Driver

This is a legacy driver which was used in DBeaver 2.x for Apache Cassandra 1.x and 2.x connectivity.  
This project is no longer supported. It is kept here for historical reasons.

### Overview

Project home: https://github.com/serge-rider/cassandra-jdbc/

Based on cassandra-jdbc project (http://code.google.com/a/apache-extras.org/p/cassandra-jdbc/)
Provides additional support of Cassandra keyspace metadata + support of older (prior 1.x) Cassandra versions.
Unnecessary dependencies removed (cassandra clientutil, google utils).

### JDBC

Driver class: org.jkiss.jdbc.cassandra.CassandraDriver

URL format: jdbc:jkiss:cassandra://HOST:PORT/KEYSPACE

This driver provides keyspace metadata information, however Cassandra isn't a relational database so
you can't work with it as with regular JDBC driver.  
Cassandra earlier than 1.x has very brief metadata information, you can't even get type of column
in column family so all columns have UTF8 names and binary values by default.  
Also when you work with resultsets it is quite tricky. ResultSet metadata renews every time you fetch next row
because any row can have any number of columns in Cassandra. The only column which present in every row is KEY
(or key alias). All other predefined column family metadata may be omitted in rows.

### License:

Apache License 2.0

### Sample

```java
import java.sql.Connection;
import java.sql.PreparedStatement;

...

    Class.forName("org.jkiss.jdbc.cassandra.CassandraDriver");
    Connection con = DriverManager.getConnection("jdbc:jkiss:cassandra://localhost:9160/Keyspace1");

    String query = "UPDATE Test SET a=?, b=? WHERE KEY=?";
    PreparedStatement statement = con.prepareStatement(query);

    statement.setLong(1, 100);
    statement.setLong(2, 1000);
    statement.setString(3, "key0");

    statement.executeUpdate();

    statement.close();
...
```

### Copyright

Jkiss driver version authored by Serge Rider
Original cassandra jdbc library authored by its respected authors (http://code.google.com/a/apache-extras.org/p/cassandra-jdbc/people/list).

