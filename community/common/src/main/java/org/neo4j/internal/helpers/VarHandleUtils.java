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
package org.neo4j.internal.helpers;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

public final class VarHandleUtils {

    private VarHandleUtils() {}

    /**
     * Produces a VarHandle giving access to a non-static field {@code name} from the calling class.
     * The VarHandle is created with {@link VarHandle#withInvokeExactBehavior()}.
     *
     * @param lookup the lookup object, must be created by the caller with {@link MethodHandles#lookup()}.
     * @param name the name of the field to create a VarHandle for.
     * @return a VarHandle to the field with the provided name from the caller class.
     */
    public static VarHandle getVarHandle(MethodHandles.Lookup lookup, String name) {
        return getVarHandle(lookup, lookup.lookupClass(), name);
    }

    /**
     * Produces a VarHandle giving access to a non-static field {@code name} from the {@code clazz} class.
     * The VarHandle is created with {@link VarHandle#withInvokeExactBehavior()}.
     *
     * @param lookup the lookup object, must be created by the caller with {@link MethodHandles#lookup()}.
     * @param clazz the class where the field is declared.
     * @param name the name of the field to create a VarHandle for.
     * @return a VarHandle to the field with the provided name from the provided class.
     */
    public static VarHandle getVarHandle(MethodHandles.Lookup lookup, Class<?> clazz, String name) {
        try {
            return getVarHandle(
                    lookup, clazz, name, clazz.getDeclaredField(name).getType());
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Produces a VarHandle giving access to a non-static field {@code name} from the {@code clazz} class with {@code type} type.
     * The VarHandle is created with {@link VarHandle#withInvokeExactBehavior()}.
     *
     * @param lookup the lookup object, must be created by the caller with {@link MethodHandles#lookup()}.
     * @param clazz the class where the field is declared.
     * @param name the name of the field to create a VarHandle for.
     * @param type the type of the field.
     * @return a VarHandle to the field with the provided name from the provided class.
     */
    public static VarHandle getVarHandle(MethodHandles.Lookup lookup, Class<?> clazz, String name, Class<?> type) {
        try {
            return lookup.findVarHandle(clazz, name, type).withInvokeExactBehavior();
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Produces a VarHandle giving access to elements of an array with {@link VarHandle#withInvokeExactBehavior()}.
     *
     * @param arrayClass the class of an array, of type T[].
     * @return a VarHandle giving access to elements of an array.
     */
    public static VarHandle arrayElementVarHandle(Class<?> arrayClass) {
        return MethodHandles.arrayElementVarHandle(arrayClass).withInvokeExactBehavior();
    }

    /**
     * Force a VarHandle to return a long. This is needed when you want to ignore the return value of
     * a VarHandle with {@link VarHandle#withInvokeExactBehavior()}.
     *
     * @param value a long value to consume.
     */
    @SuppressWarnings("unused")
    public static void consumeLong(long value) {}

    /**
     * Force a VarHandle to return an int. This is needed when you want to ignore the return value of
     * a VarHandle with {@link VarHandle#withInvokeExactBehavior()}.
     *
     * @param value an int value to consume.
     */
    @SuppressWarnings("unused")
    public static void consumeInt(int value) {}
}
