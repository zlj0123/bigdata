package io.ibigdata.flink.hbase.es.app;

import java.util.Date;

public class FundEvent {
    public String fundacco;    //varchar(12)	投资人基金帐号
    public String fundcode;    //varchar(6)	基金代码
    public String sharetype;    //varchar(1)	份额类型
    public String agencyno;    //char(3)	销售人代码
    public String netno;    //varchar(9)	托管网点号码
    public String tradeacco;    //varchar(17)	交易账号
    public Date cdate;    //date 	基金确认日期
    public Date datadate;    //	date 	数据确认日期
    public Date ddate;    //	date	交易申请日期
    public String businflag;    //	varchar(3)	业务代码
    public String cserialno;    //	varchar(20)	TA确认编号
    public String requestno;    //	varchar(24)	申请单编号
    public double confirmshares;    //	number(16,2)	交易确认份额
    public double confirmbalance;    //	number(16,2)	交易确认金额
    public String status;    //	varchar(4)	交易处理返回代码
    public String cause;    //	varchar(128)	交易处理返回信息
    public double tradefare;    //	number(10,2)	手续费
    public double tafare;    //	number(10,2)	过户费
    public double stamptax;    //	number(10,2)	印花税
    public double backfare;    //	number(10,2)	后收费用
    public double otherfare1;    //	number(10,2)	其他费用
    public double fundfare;    //	number(16,2)	基金费
    public double agencyfare;    //	number(16,2)	代销费
    public double registfare;    //	number(16,2)	管理人费用
    public String othercode;    //	varchar(6)	目标基金代码
    public String othershare;    //	char(1)	目标份额类型
    public String otheracco;    //	varchar(12)	对方TA基金帐号
    public String otheragency;    //	char(3)	对方销售人代码
    public String othernetno;    //	varchar(9)	对方托管网点号码
    public double interest;    //	number(10,2)	利息
    public double netvalue;    //	number(6,4)	成交价
    public double lastshares;    //	number(16,2)	份额余额
    public double chincome;    //	number(16,2)	兑付的未付收益
    public double chshare;    //	number(16,2)	兑付的份额
    public double confirmincome;    //	number(16,2)	未付收益
    public String outbusinflag;    //	varchar(3)	TA内部辅助业务代码
    public String shareclass;    //	char(1)	份额级别
    public double serialno;    //	number(12)	交易申请确认关联序列号
    public String moneytype;    //	varchar(3)	结算币种
    public String bonustype;    //	char(1)	分红方式
    public String freezecause;    //	char(1)	冻结原因
    public String freezeenddate;    //	char(8)	冻结截止日期
    public String foriginalno;    //	varchar(24)	原申请单编号
    public String childnetno;    //	varchar(9)	交易网点号码
    public double interestshare;    //	number(16,2)	利息转份额
    public String taflag;    //	varchar(1)	是否TA发起;
    public String tano;    //	char(2)	TA代码
    public double profit;    //	number(16,2)	业绩报酬
    public String requestflag;    //	char(1)	定期支付份额申请标志
    public String yuebao;    //	char(1)	是否来源余额宝
    public String partnerid;    //	char(20)	电商渠道
    public String rationkind;    //	varchar(1)	快溢通交易
    public String ds;    //
}
