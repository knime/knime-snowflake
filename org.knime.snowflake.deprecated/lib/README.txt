https://github.com/snowflakedb/snowflake-jdbc

The libraries in the jdbc folder are automatically downloaded from Maven 
(https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc) using the dependencies specified in the
../fetch_jars/pom.xml file. We need to use several fetch jar projects to download different versions of the same driver.

Snowflake provides a JDBC type 4 driver that supports core functionality, allowing Java program to connect to Snowflake.