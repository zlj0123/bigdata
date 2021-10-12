package io.ibigdata.flink.hbase.es.app;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Properties;

public class Kafka2HBaseMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(36);

        Properties props = new Properties();
        props.put("bootstrap.servers", "bdp-1.rdc.com:9092");
        props.put("zookeeper.connect", "bdp-1.rdc.com:2181");
        props.put("group.id", "kafka2hbase-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
                "hbasees-test",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props)).setParallelism(36);

        DataStream<FundEvent> fundEventStream = dataStreamSource.map(new MapFunction<String, FundEvent>() {
            @Override
            public FundEvent map(String value) throws Exception {
                return JSON.parseObject(value, FundEvent.class);
            }
        });

        fundEventStream.writeUsingOutputFormat(new HBaseOutputFormat()).setParallelism(36);

        env.execute("flink job kafka2hbase");
    }
}

class HBaseOutputFormat implements OutputFormat<FundEvent> {
    private org.apache.hadoop.conf.Configuration configuration;
    private Connection connection = null;
    private String taskNumber = null;
    private Table table = null;
    private int rowNumber = 0;

    @Override
    public void configure(Configuration parameters) {
        //设置配置信息
        configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "bdp-1.rdc.com:2181");
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2081");
        configuration.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "30000");
        configuration.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "30000");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        connection = ConnectionFactory.createConnection(configuration);
        TableName tableName = TableName.valueOf("hbasees-test");
        table = connection.getTable(tableName);
        this.taskNumber = String.valueOf(taskNumber);
    }

    @Override
    public void writeRecord(FundEvent event) throws IOException {
        Put put = new Put(Bytes.toBytes(new StringBuilder(event.cserialno).reverse().toString()));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("fundacco"),
                Bytes.toBytes(String.valueOf(event.fundacco)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("tradeacco"),
                Bytes.toBytes(String.valueOf(event.tradeacco)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("fundcode"),
                Bytes.toBytes(String.valueOf(event.fundcode)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("sharetype"),
                Bytes.toBytes(String.valueOf(event.sharetype)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("agencyno"),
                Bytes.toBytes(String.valueOf(event.agencyno)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("netno"),
                Bytes.toBytes(String.valueOf(event.netno)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("cdate"),
                Bytes.toBytes(String.valueOf(event.cdate)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("ddate"),
                Bytes.toBytes(String.valueOf(event.ddate)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("datadate"),
                Bytes.toBytes(String.valueOf(event.datadate)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("businflag"),
                Bytes.toBytes(String.valueOf(event.businflag)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("requestno"),
                Bytes.toBytes(String.valueOf(event.requestno)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("cserialno"),
                Bytes.toBytes(String.valueOf(event.cserialno)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("confirmshares"),
                Bytes.toBytes(String.valueOf(event.confirmshares)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("confirmbalance"),
                Bytes.toBytes(String.valueOf(event.confirmbalance)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("status"),
                Bytes.toBytes(String.valueOf(event.status)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("cause"),
                Bytes.toBytes(String.valueOf(event.cause)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("tradefare"),
                Bytes.toBytes(String.valueOf(event.tradefare)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("tafare"),
                Bytes.toBytes(String.valueOf(event.tafare)));


        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("stamptax"),
                Bytes.toBytes(String.valueOf(event.stamptax)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("backfare"),
                Bytes.toBytes(String.valueOf(event.backfare)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("otherfare1"),
                Bytes.toBytes(String.valueOf(event.otherfare1)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("fundfare"),
                Bytes.toBytes(String.valueOf(event.fundfare)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("agencyfare"),
                Bytes.toBytes(String.valueOf(event.agencyfare)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("registfare"),
                Bytes.toBytes(String.valueOf(event.registfare)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("othercode"),
                Bytes.toBytes(String.valueOf(event.othercode)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("othershare"),
                Bytes.toBytes(String.valueOf(event.othershare)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("otheracco"),
                Bytes.toBytes(String.valueOf(event.otheracco)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("otheragency"),
                Bytes.toBytes(String.valueOf(event.otheragency)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("othernetno"),
                Bytes.toBytes(String.valueOf(event.othernetno)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("interest"),
                Bytes.toBytes(String.valueOf(event.interest)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("netvalue"),
                Bytes.toBytes(String.valueOf(event.netvalue)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("lastshares"),
                Bytes.toBytes(String.valueOf(event.lastshares)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("outbusinflag"),
                Bytes.toBytes(String.valueOf(event.outbusinflag)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("shareclass"),
                Bytes.toBytes(String.valueOf(event.shareclass)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("moneytype"),
                Bytes.toBytes(String.valueOf(event.moneytype)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("bonustype"),
                Bytes.toBytes(String.valueOf(event.bonustype)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("foriginalno"),
                Bytes.toBytes(String.valueOf(event.foriginalno)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("childnetno"),
                Bytes.toBytes(String.valueOf(event.childnetno)));

        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("taflag"),
                Bytes.toBytes(String.valueOf(event.taflag)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("tano"),
                Bytes.toBytes(String.valueOf(event.tano)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("yuebao"),
                Bytes.toBytes(String.valueOf(event.yuebao)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("partnerid"),
                Bytes.toBytes(String.valueOf(event.partnerid)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("rationkind"),
                Bytes.toBytes(String.valueOf(event.rationkind)));
        put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("ds"),
                Bytes.toBytes(String.valueOf(event.ds)));

        rowNumber++;
        table.put(put);
    }

    @Override
    public void close() throws IOException {
        table.close();
        connection.close();
    }
}