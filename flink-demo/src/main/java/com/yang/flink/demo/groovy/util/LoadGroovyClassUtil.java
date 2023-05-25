package com.yang.flink.demo.groovy.util;

import groovy.lang.GroovyClassLoader;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 加载Groovy脚本中定义的类
 */
public class LoadGroovyClassUtil {

    private static GroovyClassLoader classLoader = new GroovyClassLoader();
    private static Map<String, Class<?>> classCache = new ConcurrentHashMap<>();

    public static Class<?> parseClass(String script) {

        String cacheKey = DigestUtils.md5Hex(script);
        Class<?> FunctionClass;
        if (classCache.containsKey(cacheKey)) {
            FunctionClass = classCache.get(cacheKey);
        } else {
            // 编译 Groovy 脚本文件并加载类
            FunctionClass = classLoader.parseClass(script);
            // 创建 MapFunction 实例
            classCache.put(cacheKey, FunctionClass);
        }
        return FunctionClass;
    }
}
