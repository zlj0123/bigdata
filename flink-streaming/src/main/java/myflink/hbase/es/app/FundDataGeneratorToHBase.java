package myflink.hbase.es.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;


public class FundDataGeneratorToHBase {

    public static void main(String[] args) throws InterruptedException, IOException {

        if (args.length == 0){
            System.out.println("请输入开始日期的Unix时间 - 毫秒");
            System.exit(0);
        }

        Configuration configuration;
        Connection connection = null;
        String taskNumber = null;
        Table table = null;

        //设置配置信息
        configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "bdp1:2181");
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2081");
        configuration.set(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, "30000");
        configuration.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "30000");

        connection = ConnectionFactory.createConnection(configuration);
        TableName tableName = TableName.valueOf("hbasees-test");
        table = connection.getTable(tableName);

        Random random = new Random();
        long timeStart = Long.valueOf(args[0]); //2019-07-01 00:00:00 ----- 1435680000000
        long timeEnd = 1593532799000L;  //2020-06-30 23:59:59 ----- 1593532799000

        long count = 0L;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        List<Put> puts = new ArrayList<Put>();

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

            timeStart = timeStart + 10;

            Put put = new Put(Bytes.toBytes(sdf.format(tmpDate) + new StringBuilder(event.cserialno).reverse().toString()));

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

            puts.add(put);
            count++;

            if (count%2000 == 0){
                table.put(puts);
                puts.clear();

                if (count%100000 == 0){
                    Date tmp = new Date(timeStart);
                    System.out.println("数据已经插入到:" + tmp);
                    System.out.println("总共插入的记录条数:" + count);
                }
            }
        }
    }
}
