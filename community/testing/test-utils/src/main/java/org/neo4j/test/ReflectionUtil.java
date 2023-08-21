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
package org.neo4j.test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.neo4j.util.Preconditions;

public final class ReflectionUtil {
    private ReflectionUtil() {}

    public static <T> T callCopyConstructor(T obj) {
        try {
            Class<T> objClass = (Class<T>) obj.getClass();
            return objClass.getDeclaredConstructor(objClass).newInstance(obj);
        } catch (InstantiationException
                | IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static void verifyMethodExists(Class<?> owner, String methodName) {
        Set<String> methods = new HashSet<>();
        // Includes methods declared/implemented specifically in the owner class, even private/protected and such
        Arrays.stream(owner.getDeclaredMethods()).forEach(method -> methods.add(method.getName()));
        // Includes public methods specified by interfaces and subclasses too
        Arrays.stream(owner.getMethods()).forEach(method -> methods.add(method.getName()));
        Preconditions.checkState(
                methods.stream().anyMatch(existingMethodName -> existingMethodName.equals(methodName)),
                "Method '%s' does not exist in class %s",
                methodName,
                owner);
    }

    public static List<Field> getAllFields(Class<?> baseClazz) {
        List<Field> fields = new ArrayList<>();
        Class<?> clazz = baseClazz;
        do {
            Collections.addAll(fields, clazz.getDeclaredFields());
            clazz = clazz.getSuperclass();
        } while (clazz != null);
        return fields;
    }
}
