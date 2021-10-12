package io.ibigdata.flink.data;

/**
 * @author fanrui
 * @time 2020-03-29 15:07:17
 * 商品的详细信息
 */

public class Goods {

    /**
     * 商品id
     */
    private int goodsId;

    /**
     * 价格
     */
    private String goodsName;

    /**
     * 当前商品是否被下架，如果下架应该从 State 中去移除
     * true 表示下架
     * false 表示上架
     */
    private boolean isRemove;

    public int getGoodsId() {
        return goodsId;
    }

    public void setGoodsId(int goodsId) {
        this.goodsId = goodsId;
    }

    public String getGoodsName() {
        return goodsName;
    }

    public void setGoodsName(String goodsName) {
        this.goodsName = goodsName;
    }

    public boolean isRemove() {
        return isRemove;
    }

    public void setRemove(boolean remove) {
        isRemove = remove;
    }
}
