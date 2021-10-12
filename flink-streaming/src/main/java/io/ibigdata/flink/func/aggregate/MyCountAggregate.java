package io.ibigdata.flink.func.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;

public class MyCountAggregate implements AggregateFunction<ProductViewData,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ProductViewData productViewData, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}
