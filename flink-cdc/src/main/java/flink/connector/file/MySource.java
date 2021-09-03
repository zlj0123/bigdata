package flink.connector.file;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.Date;

public class MySource implements SourceFunction<UserInfo> {

    String userids[] = {
            "4760858d-2bec-483c-a535-291de04b2247", "67088699-d4f4-43f2-913c-481bff8a2dc5",
            "72f7b6a8-e1a9-49b4-9a0b-770c41e01bfb", "dfa27cb6-bd94-4bc0-a90b-f7beeb9faa8b",
            "aabbaa50-72f4-495c-b3a1-70383ee9d6a4", "3218bbb9-5874-4d37-a82d-3e35e52d1702",
            "3ebfb9602ac07779||3ebfe9612a007979", "aec20d52-c2eb-4436-b121-c29ad4097f6c",
            "e7e896cd939685d7||e7e8e6c1930689d7", "a4b1e1db-55ef-4d9d-b9d2-18393c5f59ee"
    };

    @Override
    public void run(SourceContext<UserInfo> sourceContext) throws Exception {
        while (true) {
            String userid = userids[(int) (Math.random() * (userids.length - 1))];
            UserInfo userInfo = new UserInfo();
            userInfo.setUserId(userid);
            userInfo.setAmount(Math.random() * 100);
            userInfo.setTs(new Timestamp(new Date().getTime()));
            sourceContext.collect(userInfo);
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {

    }
}
