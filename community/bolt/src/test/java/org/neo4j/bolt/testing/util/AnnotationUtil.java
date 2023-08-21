/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.testing.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

public final class AnnotationUtil {
    private AnnotationUtil() {}

    /**
     * Locates an annotation of a given type on the processed test function or class.
     *
     * @param context an extension context.
     * @param annotationType an annotation type.
     * @return an annotation.
     * @param <A> an annotation type.
     */
    public static <A extends Annotation> Optional<A> findAnnotation(ExtensionContext context, Class<A> annotationType) {
        return context.getTestMethod()
                .flatMap(method -> AnnotationSupport.findAnnotation(method, annotationType))
                .or(() -> context.getTestClass()
                        .flatMap(clazz -> AnnotationSupport.findAnnotation(clazz, annotationType)));
    }

    public static <A extends Annotation> List<A> findAnnotations(ExtensionContext context, Class<A> annotationType) {
        var results = new ArrayList<A>();

        context.getTestClass()
                .map(clazz -> AnnotationSupport.findRepeatableAnnotations(clazz, annotationType))
                .ifPresent(results::addAll);
        context.getTestMethod()
                .map(method -> AnnotationSupport.findRepeatableAnnotations(method, annotationType))
                .ifPresent(results::addAll);

        return results;
    }

    private static <T> T instantiateProvider(Class<? extends T> type) {
        Constructor<? extends T> constructor;
        try {
            constructor = type.getDeclaredConstructor();
        } catch (NoSuchMethodException ex) {
            throw new IllegalArgumentException(
                    "Invalid provider implementation " + type.getName() + ": Missing default no-args constructor", ex);
        }

        try {
            return constructor.newInstance();
        } catch (IllegalAccessException ex) {
            throw new IllegalArgumentException(
                    "Invalid provider implementation " + type.getName() + ": Inaccessible no-args constructor", ex);
        } catch (InstantiationException | InvocationTargetException ex) {
            throw new IllegalStateException("Failed to instantiate provider implementation " + type.getName(), ex);
        }
    }

    private static <A extends Annotation, T> T instantiateProvider(
            A annotation, Function<A, Class<? extends T>> typeParameter) {
        return instantiateProvider(typeParameter.apply(annotation));
    }

    public static <A extends Annotation, T> Optional<T> selectProvider(
            ExtensionContext context, Class<A> annotationType, Function<A, Class<? extends T>> typeParameter) {
        return findAnnotation(context, annotationType)
                .map(annotation -> instantiateProvider(annotation, typeParameter));
    }

    public static <A extends Annotation, T> Optional<T> selectProvider(
            AnnotatedElement element, Class<A> annotationType, Function<A, Class<? extends T>> typeParameter) {
        return AnnotationSupport.findAnnotation(element, annotationType)
                .map(annotation -> instantiateProvider(annotation, typeParameter));
    }

    public static <A extends Annotation, T> List<T> selectProviders(
            ExtensionContext context,
            Class<A> annotationType,
            Function<A, Class<? extends T>> typeParameter,
            boolean distinct) {
        var stream = findAnnotations(context, annotationType).stream().map(typeParameter);

        if (distinct) {
            stream = stream.distinct();
        }

        return stream.map(AnnotationUtil::<T>instantiateProvider).toList();
    }

    public static <A extends Annotation, T> List<T> selectProviders(
            AnnotatedElement element,
            Class<A> annotationType,
            Function<A, Class<? extends T>> typeParameter,
            boolean distinct) {
        var stream = AnnotationSupport.findRepeatableAnnotations(element, annotationType).stream()
                .map(typeParameter);

        if (distinct) {
            stream = stream.distinct();
        }

        return stream.map(AnnotationUtil::<T>instantiateProvider).toList();
    }
}
