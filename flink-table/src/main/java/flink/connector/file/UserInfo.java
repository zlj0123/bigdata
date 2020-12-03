package flink.connector.file;

import java.sql.Timestamp;

public class UserInfo implements java.io.Serializable{
    private String userId;
    private Double amount;
    private Timestamp ts;

    public String getUserId(){
        return userId;
    }

    public void setUserId(String userId){
        this.userId = userId;
    }

    public Double getAmount(){
        return amount;
    }

    public void setAmount(Double amount){
        this.amount = amount;
    }

    public Timestamp getTs(){
        return ts;
    }

    public void setTs(Timestamp ts){
        this.ts = ts;
    }
}
