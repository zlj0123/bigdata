package myflink.split;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SplitSelectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        /*为方便测试 这里把并行度设置为1*/
        senv.setParallelism(1);

        DataStream<String> sourceData = senv.readTextFile("/Users/zhanglijun/Documents/Docs/Flink/sideOutputTest.txt");

        DataStream<PersonInfo> personStream = sourceData.map(new MapFunction<String, PersonInfo>() {
            @Override
            public PersonInfo map(String s) throws Exception {
                String[] lines = s.split(",");
                PersonInfo personInfo = new PersonInfo();
                personInfo.setName(lines[0]);
                personInfo.setProvince(lines[1]);
                personInfo.setCity(lines[2]);
                personInfo.setAge(Integer.valueOf(lines[3]));
                personInfo.setIdCard(lines[4]);
                return personInfo;
            }
        });
        //这里是用spilt-slect进行一级分流
        SplitStream<PersonInfo> splitProvinceStream = personStream.split(new OutputSelector<PersonInfo>() {
            @Override
            public Iterable<String> select(PersonInfo personInfo) {
                List<String> split = new ArrayList<>();
                if ("shandong".equals(personInfo.getProvince())) {
                    split.add("shandong");
                } else if ("jiangsu".equals(personInfo.getProvince())) {
                    split.add("jiangsu");
                }
                return split;
            }
        });
        DataStream<PersonInfo> shandong = splitProvinceStream.select("shandong");
        DataStream<PersonInfo> jiangsu = splitProvinceStream.select("jiangsu");

        /*一级分流结果*/
        shandong.map(new MapFunction<PersonInfo, String>() {
            @Override
            public String map(PersonInfo personInfo) throws Exception {
                return personInfo.toString();
            }
        }).print("山东分流结果:");
        /*一级分流结果*/
        jiangsu.map(new MapFunction<PersonInfo, String>() {
            @Override
            public String map(PersonInfo personInfo) throws Exception {
                return personInfo.toString();
            }
        }).print("江苏分流结果: ");
        senv.execute();
    }
}
