package org.apache.flink.streaming.test.type;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author ZhangLijun
 * @Title: ParameterizedTypeTest
 * @ProjectName bigdata-flink
 * @Description: TODO
 * @contact: ljzhang@icarevision.cn
 * @date 2019-01-0915:32
 */
public class ParameterizedTypeTest<T> {
    private List<T> list = null;
    private Set<T> set = null;

    private Map<String,Integer> map;

    public static void main(String[] args) throws NoSuchFieldException {
        Field listField = ParameterizedTypeTest.class.getDeclaredField("list");
        Type typeList = listField.getGenericType();
        System.out.println(typeList.getClass().getName());

        ParameterizedType lType = (ParameterizedType) typeList;
        Type[] typeArray = lType.getActualTypeArguments();
        System.out.println(typeArray[0]);


        Field setField = ParameterizedTypeTest.class.getDeclaredField("set");
        Type typeSet = setField.getGenericType();
        System.out.println(typeSet.getClass().getName());

        ParameterizedType sType = (ParameterizedType) typeSet;
        Type rawType = sType.getRawType();
        System.out.println(rawType);


    }
}
