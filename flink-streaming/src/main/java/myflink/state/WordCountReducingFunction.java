package myflink.state;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

public class WordCountReducingFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private ReducingState<Tuple2<String, Integer>> reducingState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ReducingStateDescriptor<Tuple2<String, Integer>> stateDes = new ReducingStateDescriptor<Tuple2<String, Integer>>("total_cnt", (t1, t2) -> {
            return Tuple2.of(t1.f0, t1.f1 + t2.f1);
        }, TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

        reducingState = this.getRuntimeContext().getReducingState(stateDes);
    }

    @Override
    public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
        if (reducingState != null) {
            reducingState.add(value);
        }

        return reducingState.get();
    }
}
