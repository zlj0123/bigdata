package myflink.hbase.es.app;

import java.util.Date;

public class ESIndex {
    public String fundacco;    //varchar(12)	投资人基金帐号
    public String fundcode;    //varchar(6)	基金代码
    public String agencyno;    //char(3)	销售人代码
    public String tradeacco;    //varchar(17)	交易账号
    public Date cdate;    //date 	基金确认日期
    public String cserialnoReverse;    //	varchar(20)	TA确认编号
    public String yuebao;    //	char(1)	是否来源余额宝
    public String rationkind;    //	varchar(1)	快溢通交易
}
