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
package org.neo4j.shell.svm;

import com.oracle.svm.core.annotate.Substitute;
import com.oracle.svm.core.annotate.TargetClass;
import java.lang.invoke.VarHandle;
import java.util.Map;

@TargetClass(className = "org.neo4j.memory.HeapEstimator")
final class This_is_not_the_heap_you_are_looking_for {
    @Substitute
    public static long alignObjectSize(long size) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(Long value) {
        return 0L;
    }

    @Substitute
    public static long shallowSizeOfObjectArray(int size) {
        return 0L;
    }

    @Substitute
    public static long sizeOfLongArray(int size) {
        return 0L;
    }

    @Substitute
    public static long sizeOfObjectArray(long elementSize, int size) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(byte[] arr) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(boolean[] arr) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(char[] arr) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(short[] arr) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(int[] arr) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(float[] arr) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(long[] arr) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(double[] arr) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(String[] arr) {
        return 0L;
    }

    @Substitute
    public static long sizeOfHashMap(Map<?, ?> map) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(String s) {
        return 0L;
    }

    @Substitute
    public static long sizeOf(Object o) {
        return 0L;
    }

    @Substitute
    public static long shallowSizeOf(Object[] arr) {
        return 0L;
    }

    @Substitute
    public static long shallowSizeOf(Object obj) {
        return 0L;
    }

    @Substitute
    public static long shallowSizeOfInstance(Class<?> clazz) {
        return 0L;
    }

    @Substitute
    public static long shallowSizeOfInstanceWithObjectReferences(int numberOfObjectReferences) {
        return 0L;
    }
}

@TargetClass(className = "org.neo4j.memory.RuntimeInternals")
@Substitute
final class Target_org_neo4j_memory_RuntimeInternals {
    static final boolean DEBUG_ESTIMATIONS = false;

    static final long LONG_CACHE_MIN_VALUE = 0L;
    static final long LONG_CACHE_MAX_VALUE = 0L;

    static final int HEADER_SIZE = 0;
    static final int OBJECT_ALIGNMENT = 0;
    static final boolean COMPRESSED_OOPS = false;

    static final VarHandle STRING_VALUE_ARRAY = null;
}

class Neo4jMemorySubstitutions {}
