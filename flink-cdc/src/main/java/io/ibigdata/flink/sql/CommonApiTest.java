package io.ibigdata.flink.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Types.DOUBLE;
import static org.apache.flink.table.api.Types.STRING;

public class CommonApiTest {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        //1.From DataSet
        DataSet<String> input = env.readTextFile("inout/order.csv");
        input.print();

        DataSet<Tuple4<String, String, Double, String>> orderDataSet = input.map(new MapFunction<String, Tuple4<String, String, Double, String>>() {
            @Override
            public Tuple4<String, String, Double, String> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple4<String, String, Double, String>(String.valueOf(split[0]),
                        String.valueOf(split[1]),
                        Double.valueOf(split[2]),
                        String.valueOf(split[3])
                );
            }
        });

        System.out.println("--------Create a View from a DataStream or DataSet--------");
        tableEnv.createTemporaryView("Orders", orderDataSet, $("cID"),$("cName"),$("revenue"),$("cCountry"));

        // scan registered Orders table
        Table orders = tableEnv.from("Orders");
        // compute revenue for all customers from France
        Table revenue = orders
                .filter("cCountry === 'FRANCE'")
                .groupBy("cID, cName")
                .select("cID, cName, revenue.sum AS revSum");

        TupleTypeInfo<Tuple3<String, String, Double>> tupleType = new TupleTypeInfo<>(
                STRING(),
                STRING(),
                DOUBLE());

        DataSet<Tuple3<String, String, Double>> test = tableEnv.toDataSet(revenue, tupleType);
        test.print();

        System.out.println("--------Convert a DataStream or DataSet into a Table--------");
        Table tableFromDataSet = tableEnv.fromDataSet(orderDataSet,$("cID"),$("cName"),$("revenue"),$("cCountry"));
        tableFromDataSet.filter("cCountry === 'CHINA'").printSchema();

        System.out.println("--------Convert a Table into a DataStream or DataSet--------");
        // compute revenue for all customers from France
        Table revenue2 = tableEnv.sqlQuery(
                "SELECT cID, cName, SUM(revenue) AS revSum " +
                "FROM Orders " +
                "WHERE cCountry = 'FRANCE' " +
                "GROUP BY cID, cName"
        );

        DataSet<Tuple3<String, String, Double>> test2 = tableEnv.toDataSet(revenue2, tupleType);
        test.print();


        // create an output Table
//        final Schema schema = new Schema()
//                .field("a", DataTypes.INT())
//                .field("b", DataTypes.STRING())
//                .field("c", DataTypes.DOUBLE());
//
//        tableEnv.connect(new FileSystem("/path/to/file"))
//                .withFormat(new CSV().fieldDelimiter('|').deriveSchema())
//                .withSchema(schema)
//                .createTemporaryTable("CsvSinkTable");

    }

    public static class Order {
        public String cID;
        public String cName;
        public Double revenue;
        public String cCountry;

        public Order(String cID, String cName, Double revenue, String cCountry) {
            this.cID = cID;
            this.cName = cName;
            this.revenue = revenue;
            this.cCountry = cCountry;
        }
    }
}
