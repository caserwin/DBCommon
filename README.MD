## 1 这是干啥的？
java 操作数据库时，因为每张的表结构不同，一般写一个方法来对应操作一张表。比如：现在要查有A, B两张表，
则一般要写两个方法来实现查询。代码比较冗余。。。<br>

本项目就是解决这个问题的，通过配置文件和少量代码实现增、改、查数据库。目前支持 Hive、Phoenix、Mysql三种数据库。<br>

局限：目前不支持多表查询、join 操作等，只适用于对单表操作。而且只支持 string, int, float 三种数据格式，因为现在不想考虑那么多。会慢慢更新的。

## 2 如何使用？？
1. 首先需要在application.conf中定义一个表的schema。
2. 继承 BaseRecord 类，需用户自定义 cls 属性和实现 buildFields()方法。
3. 支持Hive,mysql,phoenix 的建表、查询、更新、插入等操作。


## 3 Demo
以MysqlDAO 为例，以下为各个操作步骤示例

### 3.1 配置application.conf 文件
```
database {
  table {
    person {
      cols = "id,name,age,gender"
      type = "int,string,int,string"
      comment = "身份证,姓名,年龄,性别"
      primarykey = "id,name"
    }
  }
}
```
cols 是表字段，type 是表字段对应的各个属性，comment是每个表字段备注，primaryKey定义了表主键。

### 3.2 新建一个继承 BaseRecord 的PersonRecord类。
例如：
```
public class PersonRecord extends BaseRecord {

    public static String cls = "person";

    @Override
    public BaseRecord buildFields(String... args) {
        super.setColAndValue("id", args[0]);
        super.setColAndValue("name", args[1]);
        super.setColAndValue("age", args[2]);
        super.setColAndValue("gender", args[3]);
        return this;
    }

    @Override
    public BaseRecord buildFields(LinkedHashMap<String, String> colAndValue) {
        super.setColAndValue(colAndValue);
        return this;
    }
}
```
static cls 指定了application.conf 文件的配置，对应database.table.person。必须实现buildFields(String... args)
和buildFields(LinkedHashMap<String, String> colAndValue) 方法。
这两个方法都是设置字段和对应的值，只是设置方式不一样。


### 3.3 创建表
```
public static void main(String[] args) {
    String table = "person";
    MysqlDAO mysqlDAO = new MysqlDAO();
    // 创建表
    mysqlDAO.create(table, PersonRecord.class);
}
```
生成的建表sql如下：
```
CREATE TABLE IF NOT EXISTS `person` (`id` int(11) NOT NULL COMMENT '身份证',`name` varchar(255) NOT NULL COMMENT '姓名',`age` int(11) NOT NULL COMMENT '年龄',`gender` varchar(255) NOT NULL COMMENT '性别', PRIMARY KEY (`id`,`name`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
```
<div align=center><img width="900" height="320" src="https://github.com/caserwin/DBCommon/raw/master/pic/DBCommon_demo1.png"/></div>


### 3.4 插入表

```
public static void main(String[] args) {
    String table = "person";
    MysqlDAO mysqlDAO = new MysqlDAO();

    // 插入表
    ArrayList<BaseRecord> records = new ArrayList<>();
    records.add(new PersonRecord().buildFields("2", "erwin1", "19", "male"));
    records.add(new PersonRecord().buildFields("3", "erwin2", "29", "male"));
    records.add(new PersonRecord().buildFields("4", "erwin3", "25", "female"));
    mysqlDAO.insert(table, PersonRecord.class, records);
}
```
PreparedStatement 对应的sql如下：
```
INSERT INTO `person` (`id`,`name`,`age`,`gender`) VALUES(?,?,?,?);
```
<div align=center><img width="900" height="220" src="https://github.com/caserwin/DBCommon/raw/master/pic/DBCommon_demo2.png"/></div>

### 3.5 查询表
```
public static void main(String[] args) {
    String table = "person";
    MysqlDAO mysqlDAO = new MysqlDAO();

    // 查询表
    ArrayList<Tuple3<String, String, String>> conds = new ArrayList<>();
    conds.add(new Tuple3("name", "like", "erwin%"));

    ArrayList<PersonRecord> personRecords = mysqlDAO.select(table, PersonRecord.class, new String[]{"id", "name"}, conds);
    for (PersonRecord cr : personRecords) {
        System.out.println(cr);
    }
}
```
拼接的sql如下：
```
SELECT id,name FROM person where name like 'erwin%'
```
<div align=center><img width="1000" height="200" src="https://github.com/caserwin/DBCommon/raw/master/pic/DBCommon_demo3.png"/></div>

### 3.6 更新表
```
public static void main(String[] args) {
    String table = "person";
    MysqlDAO mysqlDAO = new MysqlDAO();

    ArrayList<Tuple2<String, String>> cols = new ArrayList<>();
    cols.add(new Tuple2("name", "caroline"));
    cols.add(new Tuple2("age", "26"));

    ArrayList<Tuple3<String, String, String>> conds = new ArrayList<>();
    conds.add(new Tuple3("gender", "=", "female"));
    int affectNum = mysqlDAO.update(table, PersonRecord.class, cols, conds);
    System.out.println("更新" + affectNum + "行！");
}
```
拼接sql输出如下：
```
UPDATE person SET name = 'caroline',age = 26 where gender = 'female'
```
<div align=center><img width="900" height="220" src="https://github.com/caserwin/DBCommon/raw/master/pic/DBCommon_demo4.png"/></div>


### 3.7 删除表
就是删除表中的字段，Code 没有实现、、就是一个套路、、不想写了、、