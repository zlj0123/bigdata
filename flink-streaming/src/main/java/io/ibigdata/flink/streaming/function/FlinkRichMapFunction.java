package io.ibigdata.flink.streaming.function;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;

public class FlinkRichMapFunction extends RichMapFunction<String,String> {

    @Override
    public String map(String value) throws Exception {
        return value;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);

        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int maxNumberOfParallelSubtasks = getRuntimeContext().getMaxNumberOfParallelSubtasks();
        int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        String globalParams1 = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap().get("global-params-1");
        String taskName = getRuntimeContext().getTaskNameWithSubtasks();

        System.out.println("indexOfThisSubtask:" + indexOfThisSubtask);
        System.out.println("maxNumberOfParallelSubtasks:" + maxNumberOfParallelSubtasks);
        System.out.println("numberOfParallelSubtasks:" + numberOfParallelSubtasks);
        System.out.println("globalParams1:" + globalParams1);
        System.out.println("taskName:" + taskName);
    }
}
