package myflink.connector.elasticsearch;

import com.alibaba.fastjson.JSON;
import myflink.data.MetricEvent;
import myflink.util.ESSinkUtil;
import myflink.util.ExecutionEnvUtil;
import myflink.util.KafkaConfigUtil;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;

import static myflink.constant.PropertiesConstants.*;

class Sink2ES7Main {
    public static void main(String[] args) throws Exception {
        //获取所有参数
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        //准备好环境
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        //从kafka读取数据
        DataStreamSource<MetricEvent> data = KafkaConfigUtil.buildSource(env);

        //从配置文件中读取 es 的地址
        List<HttpHost> esAddresses = ESSinkUtil.getEsAddresses(parameterTool.get(ELASTICSEARCH_HOSTS));
        //从配置文件中读取 bulk flush size，代表一次批处理的数量，这个可是性能调优参数，特别提醒
        int bulkSize = parameterTool.getInt(ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS, 40);
        //从配置文件中读取并行 sink 数，这个也是性能调优参数，特别提醒，这样才能够更快的消费，防止 kafka 数据堆积
        int sinkParallelism = parameterTool.getInt(STREAM_SINK_PARALLELISM, 5);

        //自己再自带的 es sink 上一层封装了下
        ESSinkUtil.addSink(esAddresses, bulkSize, sinkParallelism, data,
                (MetricEvent metric, RuntimeContext runtimeContext, RequestIndexer requestIndexer) -> {
                    requestIndexer.add(Requests.indexRequest()
                            .index(ZHISHENG + "_" + metric.getName())  //es 索引名
                            .type(ZHISHENG) //es type
                            .source(JSON.toJSONBytes(metric), XContentType.JSON));
                });
        env.execute("flink learning connectors es7");
    }

}