package com.yang.flink.demo.groovy;

import groovy.lang.GroovyClassLoader;
import org.codehaus.groovy.control.CompilerConfiguration;

import java.security.AccessController;
import java.security.PrivilegedAction;

public class GroovyUtils {

    /**
     * Compiles the given Groovy code and returns the resulting class.
     *
     * @param code the Groovy source code to compile
     * @param className the name of the class to create
     * @param config the configuration for the Groovy compiler
     * @param classLoader the class loader to use for loading dependent classes
     * @return the compiled class
     */
    public static Class<?> compile(String code, String className, CompilerConfiguration config,
                                   ClassLoader classLoader) throws Exception {
        GroovyClassLoader groovyClassLoader = AccessController.doPrivileged(
                (PrivilegedAction<GroovyClassLoader>) () -> new GroovyClassLoader(classLoader, config));
        Class<?> groovyClass = AccessController.doPrivileged(
                (PrivilegedAction<Class<?>>) () -> groovyClassLoader.parseClass(code, className));
        
        return groovyClass;
    }
}
