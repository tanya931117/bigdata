package org.example.utils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class EnumUtil {

    /**
     * 判断数值是否属于枚举类的值
     * @param str
     * @return
     */
    public static boolean isInclude(Class clzz,String str) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        boolean include = false;
        if(clzz.isEnum()){
            Object[] enumConstants = clzz.getEnumConstants();
            Method getLogtype = clzz.getMethod("getLogtype");
            for (Object enumConstant: enumConstants){
                if(getLogtype.invoke(enumConstant).equals(str)){
                    include = true;
                    break;
                }
            }
        }
        return include;
    }
}
