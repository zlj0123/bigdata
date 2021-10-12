package io.ibigdata.flink.data;

/**
 * @author fanrui
 * @time 2020-03-29 09:29:28
 * 订单的详细信息
 */

public class Order {
    /**
     * 订单发生的时间
     */
    private long time;

    /**
     * 订单 id
     */
    private String orderId;

    /**
     * 用户id
     */
    private String userId;

    /**
     * 商品id
     */
    private int goodsId;

    /**
     * 价格
     */
    private long price;

    /**
     * 城市
     */
    private int cityId;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(int goodsId) {
        this.goodsId = goodsId;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    public int getCityId() {
        return cityId;
    }

    public void setCityId(int cityId) {
        this.cityId = cityId;
    }
}
