package io.ibigdata.hadoop.hdfs;

import com.alibaba.druid.sql.SQLUtils;

public class DruidSqlparseTest {
    public static void main(String[] args) {
        String sql = "select c.acpt_id               as acpt_id,\n" +
                "       c.order_no              as order_no,\n" +
                "       c.param_data            as param_data,\n" +
                "       c.column_str            as column_str,\n" +
                "       nvl(e.acpt_remark, ' ') as remark,\n" +
                "       d.init_date             as init_date\n" +
                "from hs_rpt.rpt_bps_acpt_form d\n" +
                "         left join hs_rpt.rpt_bps_acpt_form_extend e on d.acpt_id = e.acpt_id\n" +
                "         right join\n" +
                "     (select a.acpt_id    as acpt_id,\n" +
                "             '1'          as order_no,\n" +
                "             '{\"param\":{\"unfrozen_remark\":\"' ||\n" +
                "             JSON_VALUE(LISTAGG(a.PARAM_DATA, '') WITHIN GROUP (ORDER BY a.ORDER_NO),\n" +
                "                        '$.actAssetAccountFreeze.remark') ||\n" +
                "             '\"}}'        as param_data,\n" +
                "             a.column_str as column_str\n" +
                "      from hs_rpt.rpt_bps_acpt_busin_data a\n" +
                "               left join hs_rpt.rpt_bps_acpt_form b on a.acpt_id = b.acpt_id\n" +
                "      where b.acpt_busin_id = '151021'\n" +
                "      group by a.acpt_id, a.column_str) c on d.acpt_id = c.acpt_id\n" +
                "where d.init_date = (20240524)";

        System.out.println("------------oracle-------------------");
        String dbType = "oracle";
        SQLUtils.parseSingleStatement(sql, dbType);

        System.out.println("------------oceanbase-------------------");
        dbType = "oceanbase";
        SQLUtils.parseSingleStatement(sql, dbType);
    }
}
