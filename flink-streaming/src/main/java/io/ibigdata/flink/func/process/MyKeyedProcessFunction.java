package io.ibigdata.flink.func.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MyKeyedProcessFunction extends KeyedProcessFunction<String, MessageInfo, String> {

    ValueState<String> lastStatus;
    ValueState<Long> warningTimer;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        lastStatus = getRuntimeContext().getState(new ValueStateDescriptor<>("lastStatus", String.class));
        warningTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("warning-timer", Long.class));

    }

    @Override
    public void processElement(MessageInfo value, Context ctx, Collector<String> out) throws Exception {
        /*获取*/
        String currentStatus = value.getStatus();
        Long currentTimer = warningTimer.value();


        System.out.println("currentStatus:" + currentStatus);
        System.out.println("lastStatus:" + lastStatus.value());

        /*连续两次状态都是2 宕机状态，则新建定时器 30秒后进行告警*/
        if ("DEAD".equals(currentStatus) && "DEAD".equals(lastStatus.value())) {
            long timeTs = Long.valueOf(ctx.timerService().currentProcessingTime()) + 30000L;
            ctx.timerService().registerProcessingTimeTimer(timeTs);
            warningTimer.update(timeTs);
        }
        /*如果不是连续告警，我们认为是误报警，删除定时器*/
        else if (("RUNNING".equals(currentStatus) && "DEAD".equals(lastStatus.value()))) {
            if (null != currentTimer) {
                ctx.timerService().deleteProcessingTimeTimer(currentTimer);
            }
            warningTimer.clear();
        }
        /*更新上一次的状态信息*/
        lastStatus.update(value.getStatus());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        /*输出报警信息，Regionserver两次状态监测为2 宕机*/
        out.collect("主机IP：" + ctx.getCurrentKey() + " 两次Regionserver状态监测宕机，请监测！！！");
    }
}
