package myflink.test;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args){
        Map<String,Object> test = new HashMap<>();
        test.put("test1",1);
        test.put("test2","abcd");
        test.put("test3",10.88);
        test.put("test4","fwegew");
        test.put("test5",0.22);

        System.out.println(JSON.toJSONString(test));
    }
}
