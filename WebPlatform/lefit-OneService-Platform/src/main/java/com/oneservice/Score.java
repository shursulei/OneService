package com.oneservice;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author hanfeng
 * @version 1.0
 * @date 2022/5/7 10:47
 */
public class Score {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
//        Class.forName("org.apache.calcite.jdbc.Driver");
//        Connection connection = DriverManager.getConnection("jdbc:calcite:lex=JAVA");
//        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
//        final SchemaPlus rootSchema = calciteConnection.getRootSchema();
//        rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
//        calciteConnection.setSchema("hr");  // 设置默认Schema
//        Statement statement = calciteConnection.createStatement();
//        ResultSet resultSet = statement.executeQuery(
//                "select d.deptno, min(e.empid) from hr.emps as e join hr.depts as d on e.deptno = d.deptno " +
//                        "where e.deptno = 10 group by d.deptno having count(*) > 1");
//        ResultSetMetaData metaData = resultSet.getMetaData();
//        int columnCount = metaData.getColumnCount();
//        int lineIndex = 1;
//        while(resultSet.next()){
//            System.out.println("行序号 => " + lineIndex);
//            for (int i = 1; i < columnCount + 1; i++) {
//                Object value = resultSet.getObject(i);
//                System.out.println(String.format("\t列序号 => %s, 值 => %s, 类型 => %s", i,value,metaData.getColumnTypeName(i)));
//            }
//            lineIndex += 1;
//        }
//        resultSet.close();
//        statement.close();
//        connection.close();
    }
}
