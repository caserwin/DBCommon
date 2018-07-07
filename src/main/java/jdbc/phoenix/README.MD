## pom.xml 配置
```
<dependency>
    <groupId>org.apache.phoenix</groupId>
    <artifactId>phoenix-core</artifactId>
    <version>4.13.1-HBase-1.3</version>
</dependency>

<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>2.7.1</version>
    <!--<scope>provided</scope>-->
</dependency>

<dependency>
    <groupId>org.apache.hbase</groupId>
    <artifactId>hbase-common</artifactId>
    <version>1.3.1</version>
    <!--<scope>provided</scope>-->
</dependency>
<dependency>
    <groupId>org.apache.hbase</groupId>
    <artifactId>hbase-server</artifactId>
    <version>1.3.1</version>
    <!--<scope>provided</scope>-->
</dependency>
<dependency>
    <groupId>org.apache.hbase</groupId>
    <artifactId>hbase-client</artifactId>
    <version>1.3.1</version>
    <!--<scope>provided</scope>-->
</dependency>
```

## 说明
关于phoenix insert和update 都是用一个 upsert 语法实现的。<br>
单独upsert可以实现insert语法。upsert...select...可以实现 update 语法。