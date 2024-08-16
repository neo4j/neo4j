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
package org.neo4j.memory;

import static com.sun.jna.Platform.is64Bit;
import static java.lang.Math.max;
import static org.neo4j.memory.RuntimeInternals.STRING_VALUE_ARRAY;
import static org.neo4j.memory.RuntimeInternals.stringBackingArraySize;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import org.neo4j.internal.unsafe.UnsafeUtil;

public final class HeapEstimator {
    private HeapEstimator() {}

    /**
     * Number of bytes this JVM uses to represent an object reference.
     */
    public static final int OBJECT_REFERENCE_BYTES;

    /**
     * Number of bytes to represent an object header (no fields, no alignments).
     */
    public static final int OBJECT_HEADER_BYTES;

    /**
     * Number of bytes to represent an array header (no content, but with alignments).
     */
    public static final int ARRAY_HEADER_BYTES;

    /**
     * A constant specifying the object alignment boundary inside the JVM. Objects will always take a full multiple of this constant, possibly wasting some
     * space.
     */
    public static final int OBJECT_ALIGNMENT_BYTES;

    public static final long LOCAL_TIME_SIZE;
    public static final long LOCAL_DATE_SIZE;
    public static final long OFFSET_TIME_SIZE;
    public static final long LOCAL_DATE_TIME_SIZE;
    public static final long ZONED_DATE_TIME_SIZE;

    public static final long SCOPED_MEMORY_TRACKER_SHALLOW_SIZE;

    /**
     * Sizes of primitive classes.
     */
    private static final Map<Class<?>, Integer> PRIMITIVE_SIZES;

    static {
        Map<Class<?>, Integer> primitiveSizesMap = new IdentityHashMap<>(8);
        primitiveSizesMap.put(boolean.class, 1);
        primitiveSizesMap.put(byte.class, 1);
        primitiveSizesMap.put(char.class, Character.BYTES);
        primitiveSizesMap.put(short.class, Short.BYTES);
        primitiveSizesMap.put(int.class, Integer.BYTES);
        primitiveSizesMap.put(float.class, Float.BYTES);
        primitiveSizesMap.put(double.class, Double.BYTES);
        primitiveSizesMap.put(long.class, Long.BYTES);
        PRIMITIVE_SIZES = Collections.unmodifiableMap(primitiveSizesMap);
    }

    public static final int LONG_SIZE;
    private static final int STRING_SIZE;

    static {
        if (is64Bit()) {
            OBJECT_ALIGNMENT_BYTES = RuntimeInternals.OBJECT_ALIGNMENT;
            OBJECT_REFERENCE_BYTES = RuntimeInternals.COMPRESSED_OOPS ? 4 : 8;
            OBJECT_HEADER_BYTES = RuntimeInternals.HEADER_SIZE;
            ARRAY_HEADER_BYTES = (int) alignObjectSize(OBJECT_HEADER_BYTES + Integer.BYTES);
        } else {
            // Values are fixed for 32 bit JVM
            OBJECT_ALIGNMENT_BYTES = 8;
            OBJECT_REFERENCE_BYTES = 4;
            OBJECT_HEADER_BYTES = 8;
            ARRAY_HEADER_BYTES = OBJECT_HEADER_BYTES + Integer.BYTES;
        }

        LONG_SIZE = (int) shallowSizeOfInstance(Long.class);
        STRING_SIZE = (int) shallowSizeOfInstance(String.class);

        if (RuntimeInternals.DEBUG_ESTIMATIONS) {
            System.err.println(String.format(
                    "### %s static values: ###%n" + "  NUM_BYTES_OBJECT_ALIGNMENT=%d%n"
                            + "  NUM_BYTES_OBJECT_REF=%d%n"
                            + "  NUM_BYTES_OBJECT_HEADER=%d%n"
                            + "  NUM_BYTES_ARRAY_HEADER=%d%n"
                            + "  LONG_SIZE=%d%n"
                            + "  STRING_SIZE=%d%n"
                            + "  STRING_VALUE_ARRAY=%s%n",
                    HeapEstimator.class.getName(),
                    OBJECT_ALIGNMENT_BYTES,
                    OBJECT_REFERENCE_BYTES,
                    OBJECT_HEADER_BYTES,
                    ARRAY_HEADER_BYTES,
                    LONG_SIZE,
                    STRING_SIZE,
                    STRING_VALUE_ARRAY != null));
        }

        // Calculate common used sizes
        LOCAL_TIME_SIZE = shallowSizeOfInstance(LocalTime.class);
        LOCAL_DATE_SIZE = shallowSizeOfInstance(LocalDate.class);
        OFFSET_TIME_SIZE =
                shallowSizeOfInstance(OffsetTime.class) + LOCAL_TIME_SIZE; // We ignore ZoneOffset since it's cached
        LOCAL_DATE_TIME_SIZE = shallowSizeOfInstance(LocalDateTime.class) + LOCAL_DATE_SIZE + LOCAL_TIME_SIZE;
        ZONED_DATE_TIME_SIZE = shallowSizeOfInstance(ZonedDateTime.class)
                + LOCAL_DATE_TIME_SIZE; // We ignore ZoneOffset since it's cached

        SCOPED_MEMORY_TRACKER_SHALLOW_SIZE = shallowSizeOfInstance(DefaultScopedMemoryTracker.class);
    }

    /**
     * Aligns an object size to be the next multiple of {@link #OBJECT_ALIGNMENT_BYTES}.
     */
    public static long alignObjectSize(long size) {
        return (size + OBJECT_ALIGNMENT_BYTES - 1) & -OBJECT_ALIGNMENT_BYTES;
    }

    /**
     * Return the size of the provided {@link Long} object, returning 0 if it is cached by the JVM and its shallow size otherwise.
     */
    public static long sizeOf(Long value) {
        if (value >= RuntimeInternals.LONG_CACHE_MIN_VALUE && value <= RuntimeInternals.LONG_CACHE_MAX_VALUE) {
            return 0;
        }
        return LONG_SIZE;
    }

    public static long shallowSizeOfObjectArray(int size) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + (long) OBJECT_REFERENCE_BYTES * size);
    }

    public static long sizeOfIntArray(int size) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + (long) Integer.BYTES * size);
    }

    public static long sizeOfLongArray(int size) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + (long) Long.BYTES * size);
    }

    public static long sizeOfObjectArray(long elementSize, int size) {
        return shallowSizeOfObjectArray(size) + elementSize * size;
    }

    /**
     * Returns the size in bytes of the byte[] object.
     */
    public static long sizeOf(byte[] arr) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + arr.length);
    }

    /**
     * Returns the size in bytes of the boolean[] object.
     */
    public static long sizeOf(boolean[] arr) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + arr.length);
    }

    /**
     * Returns the size in bytes of the char[] object.
     */
    public static long sizeOf(char[] arr) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + (long) Character.BYTES * arr.length);
    }

    /**
     * Returns the size in bytes of the short[] object.
     */
    public static long sizeOf(short[] arr) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + (long) Short.BYTES * arr.length);
    }

    /**
     * Returns the size in bytes of the int[] object.
     */
    public static long sizeOf(int[] arr) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + (long) Integer.BYTES * arr.length);
    }

    /**
     * Returns the size in bytes of the float[] object.
     */
    public static long sizeOf(float[] arr) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + (long) Float.BYTES * arr.length);
    }

    /**
     * Returns the size in bytes of the long[] object.
     */
    public static long sizeOf(long[] arr) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + (long) Long.BYTES * arr.length);
    }

    /**
     * Returns the size in bytes of the double[] object.
     */
    public static long sizeOf(double[] arr) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + (long) Double.BYTES * arr.length);
    }

    /**
     * Returns the size in bytes of the String[] object.
     */
    public static long sizeOf(String[] arr) {
        long size = shallowSizeOf(arr);
        for (String s : arr) {
            if (s == null) {
                continue;
            }
            size += sizeOf(s);
        }
        return size;
    }

    /**
     * Returns the estimated size of the provided map (assuming it is a {@link HashMap}).
     * This only calculates the size of the map structure, the entries are not traversed
     * and needs to be tracked separately.
     *
     * @param map to estimate size of
     * @return the estimated size of the maps internal structure.
     */
    public static long sizeOfHashMap(Map<?, ?> map) {
        final int size = map.size();
        final int tableSize = HashMapNode.tableSizeFor(size);

        return HASH_MAP_SHALLOW_SIZE
                + alignObjectSize((long) ARRAY_HEADER_BYTES + (long) OBJECT_REFERENCE_BYTES * tableSize)
                + // backing table
                HASH_MAP_NODE_SHALLOW_SIZE * size; // table entries
    }

    /**
     * Recurse only into immediate descendants.
     */
    private static final int MAX_DEPTH = 1;

    private static long sizeOfMap(Map<?, ?> map, int depth, long defSize) {
        if (map == null) {
            return 0;
        }
        long size = shallowSizeOf(map);
        if (depth > MAX_DEPTH) {
            return size;
        }
        long sizeOfEntry = -1;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (sizeOfEntry == -1) {
                sizeOfEntry = shallowSizeOf(entry);
            }
            size += sizeOfEntry;
            size += sizeOfObject(entry.getKey(), depth, defSize);
            size += sizeOfObject(entry.getValue(), depth, defSize);
        }
        return alignObjectSize(size);
    }

    private static long sizeOfCollection(Collection<?> collection, int depth, long defSize) {
        if (collection == null) {
            return 0;
        }
        long size = shallowSizeOf(collection);
        if (depth > MAX_DEPTH) {
            return size;
        }
        // assume array-backed collection and add per-object references
        size += ARRAY_HEADER_BYTES + collection.size() * OBJECT_REFERENCE_BYTES;
        for (Object o : collection) {
            size += sizeOfObject(o, depth, defSize);
        }
        return alignObjectSize(size);
    }

    private static long sizeOfObject(Object o, int depth, long defSize) {
        if (o == null) {
            return 0;
        }
        long size;
        if (o instanceof String) {
            size = sizeOf((String) o);
        } else if (o instanceof boolean[]) {
            size = sizeOf((boolean[]) o);
        } else if (o instanceof byte[]) {
            size = sizeOf((byte[]) o);
        } else if (o instanceof char[]) {
            size = sizeOf((char[]) o);
        } else if (o instanceof double[]) {
            size = sizeOf((double[]) o);
        } else if (o instanceof float[]) {
            size = sizeOf((float[]) o);
        } else if (o instanceof int[]) {
            size = sizeOf((int[]) o);
        } else if (o instanceof Long) {
            size = sizeOf((Long) o);
        } else if (o instanceof long[]) {
            size = sizeOf((long[]) o);
        } else if (o instanceof short[]) {
            size = sizeOf((short[]) o);
        } else if (o instanceof String[]) {
            size = sizeOf((String[]) o);
        } else if (o instanceof Map) {
            size = sizeOfMap((Map) o, ++depth, defSize);
        } else if (o instanceof Collection) {
            size = sizeOfCollection((Collection) o, ++depth, defSize);
        } else {
            if (defSize > 0) {
                size = defSize;
            } else {
                size = shallowSizeOf(o);
            }
        }
        return size;
    }

    /**
     * Returns the size in bytes of the String object.
     */
    public static long sizeOf(String s) {
        if (s == null) {
            return 0;
        }

        long size = STRING_SIZE + ARRAY_HEADER_BYTES + stringBackingArraySize(s);
        return alignObjectSize(size);
    }

    public static long sizeOf(Object o) {
        return sizeOfObject(o, 0, 0);
    }

    /**
     * Returns the shallow size in bytes of the Object[] object.
     */
    public static long shallowSizeOf(Object[] arr) {
        return alignObjectSize((long) ARRAY_HEADER_BYTES + (long) OBJECT_REFERENCE_BYTES * arr.length);
    }

    /**
     * Estimates a "shallow" memory usage of the given object. For arrays, this will be the memory taken by array storage (no subreferences will be followed).
     * For objects, this will be the memory taken by the fields.
     * <p>
     * JVM object alignments are also applied.
     */
    public static long shallowSizeOf(Object obj) {
        if (obj == null) {
            return 0;
        }
        final Class<?> clz = obj.getClass();
        if (clz.isArray()) {
            return shallowSizeOfArray(obj);
        } else {
            return shallowSizeOfInstance(clz);
        }
    }

    /**
     * Returns the shallow instance size in bytes an instance of the given class would occupy. This works with all conventional classes and primitive types, but
     * not with arrays (the size then depends on the number of elements and varies from object to object).
     *
     * @throws IllegalArgumentException if {@code clazz} is an array class.
     * @see #shallowSizeOf(Object)
     */
    public static long shallowSizeOfInstance(Class<?> clazz) {
        if (clazz.isArray()) {
            throw new IllegalArgumentException("This method does not work with array classes.");
        }
        if (clazz.isPrimitive()) {
            return PRIMITIVE_SIZES.get(clazz);
        }

        long size = OBJECT_HEADER_BYTES;

        // Walk type hierarchy
        for (; clazz != null; clazz = clazz.getSuperclass()) {
            for (Field f : clazz.getDeclaredFields()) {
                if (!Modifier.isStatic(f.getModifiers())) {
                    Class<?> type = f.getType();
                    int fieldSize = type.isPrimitive() ? PRIMITIVE_SIZES.get(type) : OBJECT_REFERENCE_BYTES;
                    size = max(size, UnsafeUtil.getFieldOffset(f) + fieldSize);
                }
            }
        }
        return alignObjectSize(size);
    }

    /**
     * Return the shallow size of an imaginary object that would contain the given number of object references
     */
    public static long shallowSizeOfInstanceWithObjectReferences(int numberOfObjectReferences) {
        return alignObjectSize(
                (long) OBJECT_HEADER_BYTES + (long) numberOfObjectReferences * (long) OBJECT_REFERENCE_BYTES);
    }

    /**
     * Returns true if the JVM has XX:+UseCompressedOops. Not that this is not guaranteed to be correct.
     */
    public static boolean hasCompressedOOPS() {
        return RuntimeInternals.COMPRESSED_OOPS;
    }

    /**
     * Return shallow size of any <code>array</code>.
     */
    private static long shallowSizeOfArray(Object array) {
        long size = ARRAY_HEADER_BYTES;
        final int len = Array.getLength(array);
        if (len > 0) {
            Class<?> arrayElementClazz = array.getClass().getComponentType();
            if (arrayElementClazz.isPrimitive()) {
                size += (long) len * PRIMITIVE_SIZES.get(arrayElementClazz);
            } else {
                size += (long) OBJECT_REFERENCE_BYTES * len;
            }
        }
        return alignObjectSize(size);
    }

    private static final long HASH_MAP_SHALLOW_SIZE = shallowSizeOfInstance(HashMap.class);
    public static final long HASH_MAP_NODE_SHALLOW_SIZE = shallowSizeOfInstance(HashMapNode.class);

    @SuppressWarnings("unused")
    private static class HashMapNode {
        int hash;
        Object key;
        Object value;
        Object next;

        static int tableSizeFor(int cap) {
            int n = -1 >>> Integer.numberOfLeadingZeros(cap - 1);
            return (n < 0) ? 1 : (n >= (1 << 30)) ? (1 << 30) : n + 1;
        }
    }
}
