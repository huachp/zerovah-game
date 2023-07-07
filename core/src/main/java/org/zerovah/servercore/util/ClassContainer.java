package org.zerovah.servercore.util;

import org.apache.commons.lang3.ArrayUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class ClassContainer {

    private final Set<Class<?>> allClasses;

    public ClassContainer(String basePackages) {
        this.allClasses = new HashSet<>();
        ClassPathScanner scanner = new ClassPathScanner(true, true, null);
        String[] bps = basePackages.split(",");
        for (String basePackage : bps) {
            Set<Class<?>> packageAllClasses = scanner.getPackageAllClasses(basePackage, true);
            if (packageAllClasses != null && !packageAllClasses.isEmpty()) {
                this.allClasses.addAll(packageAllClasses);
            }
        }
    }

    public void addAll(ClassContainer classes) {
        this.allClasses.addAll(classes.allClasses);
    }

    public Collection<Class<?>> getClassByType(Class<?> clazzType) {
        Set<Class<?>> clazzes = new HashSet<>();
        for (Class<?> clazz : allClasses) {
            if (clazz != null && !clazz.isInterface() && instanceofClass(clazzType, clazz)) {
                clazzes.add(clazz);
            }
        }
        return clazzes;
    }

    public Collection<Class<?>> getClassByAnnotationClass(Class<? extends Annotation> annotationClass) {
        Set<Class<?>> clazzes = new HashSet<>();
        for (Class<?> clazz : allClasses) {
            if (clazz != null && !clazz.isInterface()) {
                if (clazz.isAnnotationPresent(annotationClass)) {
                    clazzes.add(clazz);
                }
            }
        }
        return clazzes;
    }

    public Collection<Class<?>> getClassByAnnotation(Annotation annotation) {
        Set<Class<?>> clazzes = new HashSet<>();
        for (Class<?> clazz : allClasses) {
            if (clazz != null && !clazz.isInterface()) {
                Annotation[] annotations = clazz.getAnnotations();
                if (annotations != null && ArrayUtils.contains(annotations, annotation)) {
                    clazzes.add(clazz);
                }
            }
        }
        return clazzes;
    }

    private boolean instanceofClass(Class<?> clazzType, Class<?> checkCls) {
        if (checkCls == null) {
            return false;
        }
        if (checkCls == Object.class) {
            return false;
        }
        if (clazzType == checkCls) {
            return false;
        }
        if (Modifier.isAbstract(checkCls.getModifiers())) {
            return false;
        }
        return clazzType.isAssignableFrom(checkCls);
    }

}
