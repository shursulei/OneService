package com.oneservice;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;

/**
 * @author hanfeng
 * @version 1.0
 * @date 2022/5/7 10:03
 */
public class Core_Test {
    public static void main(String[] args) throws SqlParseException, ValidationException, RelConversionException {
//        String sql = "\n" +
//                "INSERT INTO tmp_node\n" +
//                "SELECT s1.id1, s1.id2, s2.val1\n" +
//                "FROM source1 as s1 LEFT JOIN source2 AS s2\n" +
//                "ON s1.id1 = s2.id1 and s1.id2 = s2.id2 and s1.id3 = 5";
        String sql="select \n" +
                "  t0.tree_id, \n" +
                "  sum(t0.gap) as num \n" +
                "from \n" +
                "  (\n" +
                "    SELECT \n" +
                "      w.tree_id, \n" +
                "      w.gap, \n" +
                "      r.executed_sql \n" +
                "    FROM \n" +
                "      data_middleground.view_mkt_node_kpi_warning w \n" +
                "      JOIN data_middleground.view_mkt_node_result r ON w.tree_id = r.tree_id \n" +
                "    WHERE \n" +
                "      w.warning_status = 0 \n" +
                "      and r.is_del = 0\n" +
                "  ) t0 \n" +
                "where \n" +
                "  t0.gap > 1 \n" +
                "group by \n" +
                "  t0.tree_id \n" +
                "order by \n" +
                "  tree_id desc";
        SqlParser.Config config = SqlParser.configBuilder().build();
        SqlParser parser = SqlParser.create(sql, config);
        SqlNode sqlNode = parser.parseQuery();
        Planner planner= Frameworks.getPlanner((FrameworkConfig) config);
        SqlNode node=planner.validate(sqlNode);
        RelRoot relRoot=planner.rel(node);
        RelNode project=relRoot.project();
        System.out.println(node.toString());
    }
}
