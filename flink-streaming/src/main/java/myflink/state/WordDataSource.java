package myflink.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class WordDataSource extends RichSourceFunction<String> {
    private Boolean isCancel;
    private String[] words = {"hadoop", "spark", "linux", "flink", "flume", "oozie", "kylin"};
    private Random r;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.isCancel = false;
        this.r = new Random();
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (!this.isCancel) {
            ctx.collect(words[r.nextInt(words.length)]);
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        this.isCancel = true;
    }
}
