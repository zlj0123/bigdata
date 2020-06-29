package myflink.hbase.es.app;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class FundDataGeneratorToKafka {
    public static void main(String[] args) throws InterruptedException {
        // 构造一个java.util.Properties对象
        Properties props = new Properties();
        // 指定bootstrap.servers属性。必填，无默认值。用于创建向kafka broker服务器的连接。
        props.put("bootstrap.servers", "10.20.30.112:9092");
        // 指定key.serializer属性。必填，无默认值。被发送到broker端的任何消息的格式都必须是字节数组。
        // 因此消息的各个组件都必须首先做序列化，然后才能发送到broker。该参数就是为消息的key做序列化只用的。
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 指定value.serializer属性。必填，无默认值。和key.serializer类似。此被用来对消息体即消息value部分做序列化。
        // 将消息value部分转换成字节数组。
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //acks参数用于控制producer生产消息的持久性（durability）。参数可选值，0、1、-1（all）。
        props.put("acks", "1");
        //props.put(ProducerConfig.ACKS_CONFIG, "1");
        //在producer内部自动实现了消息重新发送。默认值0代表不进行重试。
        props.put("retries", 3);
        //props.put(ProducerConfig.RETRIES_CONFIG, 3);
        //调优producer吞吐量和延时性能指标都有非常重要作用。默认值16384即16KB。
        props.put("batch.size", 323840);
        //props.put(ProducerConfig.BATCH_SIZE_CONFIG, 323840);
        //控制消息发送延时行为的，该参数默认值是0。表示消息需要被立即发送，无须关系batch是否被填满。
        props.put("linger.ms", 10);
        //props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        //指定了producer端用于缓存消息的缓冲区的大小，单位是字节，默认值是33554432即32M。
        props.put("buffer.memory", 33554432);
        //props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put("max.block.ms", 3000);
        //props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);
        //设置producer段是否压缩消息，默认值是none。即不压缩消息。GZIP、Snappy、LZ4
        //props.put("compression.type", "none");
        //props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        //该参数用于控制producer发送请求的大小。producer端能够发送的最大消息大小。
        //props.put("max.request.size", 10485760);
        //props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 10485760);
        //producer发送请求给broker后，broker需要在规定时间范围内将处理结果返还给producer。默认30s
        //props.put("request.timeout.ms", 60000);
        //props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000);

        // 使用上面创建的Properties对象构造KafkaProducer对象
        //如果采用这种方式创建producer，那么就不需要显示的在Properties中指定key和value序列化类了呢。
        // Serializer<String> keySerializer = new StringSerializer();
        // Serializer<String> valueSerializer = new StringSerializer();
        // Producer<String, String> producer = new KafkaProducer<String, String>(props,
        // keySerializer, valueSerializer);
        int count = 0;
        Random random = new Random();
        Producer<String, String> producer = new KafkaProducer<>(props);
        long timeStart = 1435680000000L; //2015-07-01 00:00:00 ----- 1435680000000
        long timeEnd = 1593532799000L;  //2020-06-30 23:59:59 ----- 1593532799000
        while (timeStart <= timeEnd) {
            //构造好kafkaProducer实例以后，下一步就是构造消息实例。
            FundEvent event = new FundEvent();
            //投资人基金帐号，假设总共1000万个基金账号
            //交易账号同基金账号
            long l = 51100000000L + random.nextInt(10000000);
            event.fundacco = String.valueOf(l);
            event.tradeacco = String.valueOf(l);

            //基金代码，假设总共5000个基金代码
            int i = random.nextInt(5000);
            i = i + 50000;
            event.fundcode = String.valueOf(i);

            event.sharetype = "A";

            //销售人代码
            //托管网点号码
            i = random.nextInt(300) + 1;
            event.agencyno = String.valueOf(i);
            event.netno = String.valueOf(i);

            //基金确认日期 数据确认日期 交易申请日期 设置为相同
            Date tmpDate = new Date(timeStart);
            event.cdate = tmpDate;
            event.ddate = tmpDate;
            event.datadate = tmpDate;

            event.businflag = String.valueOf(random.nextInt(50) + 100);

            //申请单编号
            event.requestno = String.valueOf(timeStart);
            //TA确认编号
            event.cserialno = String.valueOf(timeStart + 1);

            //	number(16,2)	交易确认份额
            event.confirmshares = random.nextDouble() * 1000;
            //	number(16,2)	交易确认金额
            event.confirmbalance = random.nextDouble() * 1000 * 1.1;

            event.status = "0";
            event.cause = "1";

            double fare = random.nextDouble() * 50;
            event.tradefare = fare;    //	number(10,2)	手续费
            event.tafare = fare;    //	number(10,2)	过户费
            event.stamptax = 0.0;    //	number(10,2)	印花税
            event.backfare = 0.0;    //	number(10,2)	后收费用
            event.otherfare1 = 0.0;    //	number(10,2)	其他费用
            event.fundfare = 0.0;    //	number(16,2)	基金费
            event.agencyfare = 0.0;    //	number(16,2)	代销费
            event.registfare = 0.0;    //	number(16,2)	管理人费用

            event.othercode="";    //	varchar(6)	目标基金代码
            event.othershare = "A";    //	char(1)	目标份额类型
            event.otheracco="";    //	varchar(12)	对方TA基金帐号
            event.otheragency="";    //	char(3)	对方销售人代码
            event.othernetno="";    //	varchar(9)	对方托管网点号码

            event.interest=0;    //	number(10,2)	利息
            event.netvalue=random.nextDouble() * 3;    //	number(6,4)	成交价
            event.lastshares=random.nextDouble() * 100000;    //	number(16,2)	份额余额

            event.outbusinflag = String.valueOf(random.nextInt(50) + 100);;    //	varchar(3)	TA内部辅助业务代码
            event.shareclass="";    //	char(1)	份额级别

            event.moneytype = "156";    //	varchar(3)	结算币种
            event.bonustype = "0";

            event.foriginalno = String.valueOf(timeStart);
            event.childnetno = event.netno;

            event.taflag = "0";    //	varchar(1)	是否TA发起;
            event.tano = "5";    //	char(2)	TA代码
            event.yuebao = String.valueOf(random.nextInt(5));    //	char(1)	是否来源余额宝
            event.partnerid = "";    //	char(20)	电商渠道
            event.rationkind = String.valueOf(random.nextInt(5));   //	varchar(1)	快溢通交易
            event.ds = "test";    //

            producer.send(new ProducerRecord<>("hbasees-test", Integer.toString(i), JSON.toJSONString(event)));
            timeStart = timeStart + 5;
        }
    }
}
