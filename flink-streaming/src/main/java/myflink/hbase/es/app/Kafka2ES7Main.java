package myflink.hbase.es.app;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Kafka2ES7Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(36);

        Properties props = new Properties();
        props.put("bootstrap.servers", "bdp-1.rdc.com:9092");
        props.put("zookeeper.connect", "bdp-1.rdc.com:2181");
        props.put("group.id", "kafka2es-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer011<>(
                "hbasees-test",  //kafka topic
                new SimpleStringSchema(),  // String 序列化
                props)).setParallelism(36);

        DataStream<FundEvent> fundEventStream = dataStreamSource.map(new MapFunction<String, FundEvent>() {
            @Override
            public FundEvent map(String value) throws Exception {
                return JSON.parseObject(value, FundEvent.class);
            }
        });

        List<HttpHost> esAddresses = getEsAddresses("bdp-1.rdc.com:9200,bdp-2.rdc.com:9200,bdp-3.rdc.com:9200");
        int bulkSize = 100;

        ElasticsearchSink.Builder<FundEvent> esSinkBuilder = new ElasticsearchSink.Builder<>(esAddresses, new ESSinkFunc());
        esSinkBuilder.setBulkFlushMaxActions(bulkSize);
        fundEventStream.addSink(esSinkBuilder.build()).setParallelism(36);

        env.execute("flink job kafka2es7");
    }

    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }
}

class ESSinkFunc implements ElasticsearchSinkFunction<FundEvent> {

    @Override
    public void process(FundEvent element, RuntimeContext ctx, RequestIndexer indexer) {

        ESIndex esIndex = new ESIndex();
        esIndex.agencyno = element.agencyno;
        esIndex.cdate = element.cdate;
        esIndex.cserialnoReverse = new StringBuilder(element.cserialno).reverse().toString();
        esIndex.fundacco = element.fundacco;
        esIndex.fundcode = element.fundcode;
        esIndex.rationkind = element.rationkind;
        esIndex.yuebao = element.yuebao;
        esIndex.tradeacco = element.tradeacco;

        indexer.add(Requests.indexRequest()
                .index("fund_" + new SimpleDateFormat("yyyyMM").format(element.cdate))
                .type("_doc")
                .source(JSON.toJSONBytes(esIndex), XContentType.JSON));
    }
}