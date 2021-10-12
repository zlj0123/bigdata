package io.ibigdata.flink.func.aggregate;

public class ProductViewData {
    public String productId;
    public String userId;
    public long operationType;
    public long timestamp;

    public ProductViewData(String productId, String userId, long operationType, long timestamp) {
        this.productId = productId;
        this.userId = userId;
        this.operationType = operationType;
        this.timestamp = timestamp;
    }
}
