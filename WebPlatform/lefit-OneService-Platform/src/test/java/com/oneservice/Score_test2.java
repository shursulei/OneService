package com.oneservice;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;

/**
 * @author hanfeng
 * @version 1.0
 * @date 2022/5/7 11:50
 */
public class Score_test2 {
    public static void main(String[] args) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        .setParserFactory(SqlParserImpl.FACTORY)
                        .setCaseSensitive(false)
                        .setQuoting(Quoting.BACK_TICK)
                        .setQuotedCasing(Casing.TO_UPPER)
                        .setUnquotedCasing(Casing.TO_UPPER)
                        .setConformance(SqlConformanceEnum.ORACLE_12)
                        .build())
                .build();



        String sql = "select ids, name, rank() over (PARTITION BY ids ORDER BY name) from test where id < 5 and name = 'zhang'";
        SqlParser parser = SqlParser.create(sql, config.getParserConfig());
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println(sqlNode.toString());
            Planner planner=Frameworks.getPlanner(config);
            Planner node= (Planner) planner.validate(sqlNode);
            RelRoot relRoot=planner.rel((SqlNode) node);
            RelNode project=relRoot.project();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
