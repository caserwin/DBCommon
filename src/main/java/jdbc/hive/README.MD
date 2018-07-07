## maven pom 配置
```
<dependency>
    <groupId>org.apache.hive</groupId>
    <artifactId>hive-jdbc</artifactId>
    <version>1.1.0</version>
</dependency>

<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <version>2.7.1</version>
</dependency>

<dependency>
    <groupId>org.apache.httpcomponents</groupId>
    <artifactId>httpclient</artifactId>
    <version>4.5.2</version>
</dependency>
```

## 示例demo:
```
import jdbc.app.record.PersonRecord;
import jdbc.common.BaseRecord;
import jdbc.common.tuple.Tuple3;
import jdbc.hive.HiveDAO;
import java.util.ArrayList;

/**
 * Created by yidxue on 2018/7/4
 */
public class HiveTestAPP {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("args error!");
            return;
        }
        String table = args[0];
        String path = args[1];

        HiveDAO hiveDAO = new HiveDAO();
        ArrayList<BaseRecord> records = new ArrayList<>();
        records.add(new PersonRecord().buildFields("2", "erwin1", "19", "male"));
        records.add(new PersonRecord().buildFields("3", "erwin2", "29", "male"));
        records.add(new PersonRecord().buildFields("4", "erwin3", "25", "female"));

        // 创建表
        hiveDAO.create(table, PersonRecord.class);
        // 插入表
        hiveDAO.loadToHive(records, PersonRecord.class, table, path, true, true);
        // 查询表
        ArrayList<Tuple3<String, String, String>> conds = new ArrayList<>();
        conds.add(new Tuple3("name", "like", "erwin%"));
        conds.add(new Tuple3("id", "in", "(1,2,3)"));

        ArrayList<PersonRecord> personRecords = hiveDAO.select(table, PersonRecord.class, new String[]{"id", "name"}, conds);
        for (PersonRecord ps : personRecords) {
            System.out.println(ps);
        }
    }
}
```

## 结果示例
HiveTestAPP 执行结果如下展示：
<div align=left><img width="1000" height="130" src="https://github.com/caserwin/DBCommon/raw/master/pic/DBCommon_demo5.png"/></div>

## 说明
先创建表，再通过load语法加载数据。这里时load local的文件到Hive，所以必须注意路径的权限问题，每一层级路径的权限最好都是 777。最后查询数据。


## 注意点
Hive 必须通过配置才能支持行级别的插入、删除和更新。否则会报如下错误：
```
org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: SemanticException [Error 10294]: Attempt to do update or delete using transaction manager that does not support these operations.
	at org.apache.hive.jdbc.Utils.verifySuccess(Utils.java:231)
	at org.apache.hive.jdbc.Utils.verifySuccessWithInfo(Utils.java:217)
	at org.apache.hive.jdbc.HiveStatement.execute(HiveStatement.java:254)
	at org.apache.hive.jdbc.HiveStatement.executeUpdate(HiveStatement.java:406)
	at jdbc.common.SQLUtil.update(SQLUtil.java:64)
	at jdbc.hive.HiveDAO.update(HiveDAO.java:56)
	at jdbc.app.HiveTestAPP.main(HiveTestAPP.java:49)
Caused by: org.apache.hive.service.cli.HiveSQLException: Error while compiling statement: FAILED: SemanticException [Error 10294]: Attempt to do update or delete using transaction manager that does not support these operations.
	at org.apache.hive.service.cli.operation.Operation.toSQLException(Operation.java:400)
	at org.apache.hive.service.cli.operation.SQLOperation.prepare(SQLOperation.java:187)
	at org.apache.hive.service.cli.operation.SQLOperation.runInternal(SQLOperation.java:271)
	at org.apache.hive.service.cli.operation.Operation.run(Operation.java:337)
	at org.apache.hive.service.cli.session.HiveSessionImpl.executeStatementInternal(HiveSessionImpl.java:439)
	at org.apache.hive.service.cli.session.HiveSessionImpl.executeStatementAsync(HiveSessionImpl.java:416)
	at sun.reflect.GeneratedMethodAccessor40.invoke(Unknown Source)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:498)
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:78)
	at org.apache.hive.service.cli.session.HiveSessionProxy.access$000(HiveSessionProxy.java:36)
	at org.apache.hive.service.cli.session.HiveSessionProxy$1.run(HiveSessionProxy.java:63)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:422)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1920)
	at org.apache.hive.service.cli.session.HiveSessionProxy.invoke(HiveSessionProxy.java:59)
	at com.sun.proxy.$Proxy20.executeStatementAsync(Unknown Source)
	at org.apache.hive.service.cli.CLIService.executeStatementAsync(CLIService.java:282)
	at org.apache.hive.service.cli.thrift.ThriftCLIService.ExecuteStatement(ThriftCLIService.java:501)
	at org.apache.hive.service.cli.thrift.TCLIService$Processor$ExecuteStatement.getResult(TCLIService.java:1313)
	at org.apache.hive.service.cli.thrift.TCLIService$Processor$ExecuteStatement.getResult(TCLIService.java:1298)
	at org.apache.thrift.ProcessFunction.process(ProcessFunction.java:39)
	at org.apache.thrift.TBaseProcessor.process(TBaseProcessor.java:39)
	at org.apache.hive.service.auth.TSetIpAddressProcessor.process(TSetIpAddressProcessor.java:56)
	at org.apache.thrift.server.TThreadPoolServer$WorkerProcess.run(TThreadPoolServer.java:286)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
Caused by: java.lang.RuntimeException: org.apache.hadoop.hive.ql.parse.SemanticException:Attempt to do update or delete using transaction manager that does not support these operations.
	at org.apache.hadoop.hive.ql.parse.UpdateDeleteSemanticAnalyzer.analyzeInternal(UpdateDeleteSemanticAnalyzer.java:65)
	at org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.analyze(BaseSemanticAnalyzer.java:223)
	at org.apache.hadoop.hive.ql.Driver.compile(Driver.java:558)
	at org.apache.hadoop.hive.ql.Driver.compileInternal(Driver.java:1356)
	at org.apache.hadoop.hive.ql.Driver.compileAndRespond(Driver.java:1343)
	at org.apache.hive.service.cli.operation.SQLOperation.prepare(SQLOperation.java:185)
```

所以这里我就实现了hive的load语法。insert、update先偷懒不写了。