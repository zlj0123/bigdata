package io.ibigdata.flink.connector.tidb;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class StudentSource implements SourceFunction<Student> {

    private int i = 0;

    @Override
    public void run(SourceContext<Student> sourceContext) throws Exception {
        while (true) {
            Student student = new Student(i++, "name" + i, "pw" + i, i);
            sourceContext.collect(student);
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {

    }
}
