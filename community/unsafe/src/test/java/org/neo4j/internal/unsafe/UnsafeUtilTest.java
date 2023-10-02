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
package org.neo4j.internal.unsafe;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.unsafe.UnsafeUtil.allocateMemory;
import static org.neo4j.internal.unsafe.UnsafeUtil.arrayBaseOffset;
import static org.neo4j.internal.unsafe.UnsafeUtil.arrayIndexScale;
import static org.neo4j.internal.unsafe.UnsafeUtil.arrayOffset;
import static org.neo4j.internal.unsafe.UnsafeUtil.assertHasUnsafe;
import static org.neo4j.internal.unsafe.UnsafeUtil.compareAndSwapLong;
import static org.neo4j.internal.unsafe.UnsafeUtil.free;
import static org.neo4j.internal.unsafe.UnsafeUtil.getAndSetLong;
import static org.neo4j.internal.unsafe.UnsafeUtil.getByte;
import static org.neo4j.internal.unsafe.UnsafeUtil.getFieldOffset;
import static org.neo4j.internal.unsafe.UnsafeUtil.getInt;
import static org.neo4j.internal.unsafe.UnsafeUtil.getLong;
import static org.neo4j.internal.unsafe.UnsafeUtil.getLongVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.getShort;
import static org.neo4j.internal.unsafe.UnsafeUtil.initDirectByteBuffer;
import static org.neo4j.internal.unsafe.UnsafeUtil.newDirectByteBuffer;
import static org.neo4j.internal.unsafe.UnsafeUtil.pageSize;
import static org.neo4j.internal.unsafe.UnsafeUtil.putByte;
import static org.neo4j.internal.unsafe.UnsafeUtil.putInt;
import static org.neo4j.internal.unsafe.UnsafeUtil.putLong;
import static org.neo4j.internal.unsafe.UnsafeUtil.putLongVolatile;
import static org.neo4j.internal.unsafe.UnsafeUtil.putShort;
import static org.neo4j.internal.unsafe.UnsafeUtil.setMemory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.neo4j.memory.LocalMemoryTracker;

class UnsafeUtilTest {
    static class Obj {
        boolean aBoolean;
        byte aByte;
        short aShort;
        float aFloat;
        char aChar;
        int anInt;
        long aLong;
        double aDouble;
        Object object;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Obj obj = (Obj) o;
            return aBoolean == obj.aBoolean
                    && aByte == obj.aByte
                    && aShort == obj.aShort
                    && Float.compare(obj.aFloat, aFloat) == 0
                    && aChar == obj.aChar
                    && anInt == obj.anInt
                    && aLong == obj.aLong
                    && Double.compare(obj.aDouble, aDouble) == 0
                    && Objects.equals(object, obj.object);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aBoolean, aByte, aShort, aFloat, aChar, anInt, aLong, aDouble, object);
        }
    }

    @Test
    void mustHaveUnsafe() {
        assertHasUnsafe();
    }

    @Test
    void pageSizeIsPowerOfTwo() {
        assertThat(pageSize())
                .isIn(
                        1,
                        2,
                        4,
                        8,
                        16,
                        32,
                        64,
                        128,
                        256,
                        512,
                        1024,
                        2048,
                        4096,
                        8192,
                        16384,
                        32768,
                        65536,
                        131072,
                        262144,
                        524288,
                        1048576,
                        2097152,
                        4194304,
                        8388608,
                        16777216,
                        33554432,
                        67108864,
                        134217728,
                        268435456,
                        536870912,
                        1073741824);
    }

    @Test
    void mustSupportReadingFromAndWritingToFields() {
        Obj obj;

        long aByteOffset = getFieldOffset(Obj.class, "aByte");
        obj = new Obj();
        putByte(obj, aByteOffset, (byte) 1);
        assertThat(obj.aByte).isEqualTo((byte) 1);
        assertThat(getByte(obj, aByteOffset)).isEqualTo((byte) 1);
        obj.aByte = 0;
        assertThat(obj).isEqualTo(new Obj());
    }

    @Test
    void mustSupportReadingAndWritingOfPrimitivesToMemory() {
        int sizeInBytes = 8;
        var tracker = new LocalMemoryTracker();
        long address = allocateMemory(sizeInBytes, tracker);
        try {
            putByte(address, (byte) 1);
            assertThat(getByte(address)).isEqualTo((byte) 1);
            setMemory(address, sizeInBytes, (byte) 0);
            assertThat(getByte(address)).isEqualTo((byte) 0);

            putShort(address, (short) 1);
            assertThat(getShort(address)).isEqualTo((short) 1);
            setMemory(address, sizeInBytes, (byte) 0);
            assertThat(getShort(address)).isEqualTo((short) 0);

            putInt(address, 1);
            assertThat(getInt(address)).isEqualTo(1);
            setMemory(address, sizeInBytes, (byte) 0);
            assertThat(getInt(address)).isEqualTo(0);

            putLong(address, 1);
            assertThat(getLong(address)).isEqualTo(1L);
            setMemory(address, sizeInBytes, (byte) 0);
            assertThat(getLong(address)).isEqualTo(0L);

            putLongVolatile(address, 1);
            assertThat(getLongVolatile(address)).isEqualTo(1L);
            setMemory(address, sizeInBytes, (byte) 0);
            assertThat(getLongVolatile(address)).isEqualTo(0L);
        } finally {
            free(address, sizeInBytes, tracker);
        }
    }

    @Test
    void compareAndSwapLongField() {
        Obj obj = new Obj();
        long aLongOffset = getFieldOffset(Obj.class, "aLong");
        assertTrue(compareAndSwapLong(obj, aLongOffset, 0, 5));
        assertFalse(compareAndSwapLong(obj, aLongOffset, 0, 5));
        assertTrue(compareAndSwapLong(obj, aLongOffset, 5, 0));
        assertThat(obj).isEqualTo(new Obj());
    }

    @Test
    void getAndSetLongField() {
        Obj obj = new Obj();
        long offset = getFieldOffset(Obj.class, "aLong");
        assertThat(getAndSetLong(obj, offset, 42L)).isEqualTo(0L);
        assertThat(getAndSetLong(obj, offset, -1)).isEqualTo(42L);
    }

    @Test
    void unsafeArrayElementAccess() {
        int len = 3;
        int scale;
        int base;

        byte[] bytes = new byte[len];
        scale = arrayIndexScale(bytes.getClass());
        base = arrayBaseOffset(bytes.getClass());
        putByte(bytes, arrayOffset(1, base, scale), (byte) -1);
        assertThat(bytes[0]).isEqualTo((byte) 0);
        assertThat(bytes[1]).isEqualTo((byte) -1);
        assertThat(bytes[2]).isEqualTo((byte) 0);
    }

    @Test
    void shouldPutAndGetByteWiseLittleEndianShort() {
        // GIVEN
        int sizeInBytes = 2;
        var tracker = new LocalMemoryTracker();
        long p = allocateMemory(sizeInBytes, tracker);
        short value = (short) 0b11001100_10101010;

        // WHEN
        UnsafeUtil.putShortByteWiseLittleEndian(p, value);
        short readValue = UnsafeUtil.getShortByteWiseLittleEndian(p);

        // THEN
        free(p, sizeInBytes, tracker);
        assertEquals(value, readValue);
    }

    @Test
    void shouldPutAndGetByteWiseLittleEndianInt() {
        // GIVEN
        int sizeInBytes = 4;
        var tracker = new LocalMemoryTracker();
        long p = allocateMemory(sizeInBytes, tracker);
        int value = 0b11001100_10101010_10011001_01100110;

        // WHEN
        UnsafeUtil.putIntByteWiseLittleEndian(p, value);
        int readValue = UnsafeUtil.getIntByteWiseLittleEndian(p);

        // THEN
        free(p, sizeInBytes, tracker);
        assertEquals(value, readValue);
    }

    @Test
    void shouldPutAndGetByteWiseLittleEndianLong() {
        // GIVEN
        int sizeInBytes = 8;
        var tracker = new LocalMemoryTracker();
        long p = allocateMemory(sizeInBytes, tracker);
        long value = 0b11001100_10101010_10011001_01100110__10001000_01000100_00100010_00010001L;

        // WHEN
        UnsafeUtil.putLongByteWiseLittleEndian(p, value);
        long readValue = UnsafeUtil.getLongByteWiseLittleEndian(p);

        // THEN
        free(p, sizeInBytes, tracker);
        assertEquals(value, readValue);
    }

    @Test
    void directByteBufferCreationAndInitialisation() throws Throwable {
        int sizeInBytes = 313;
        var tracker = new LocalMemoryTracker();
        long address = allocateMemory(sizeInBytes, tracker);
        try {
            setMemory(address, sizeInBytes, (byte) 0);
            ByteBuffer a = newDirectByteBuffer(address, sizeInBytes);
            assertThat(a).isNotSameAs(newDirectByteBuffer(address, sizeInBytes));
            assertThat(a.hasArray()).isEqualTo(false);
            assertThat(a.isDirect()).isEqualTo(true);
            assertThat(a.capacity()).isEqualTo(sizeInBytes);
            assertThat(a.limit()).isEqualTo(sizeInBytes);
            assertThat(a.position()).isEqualTo(0);
            assertThat(a.remaining()).isEqualTo(sizeInBytes);
            assertThat(getByte(address)).isEqualTo((byte) 0);
            a.put((byte) -1);
            assertThat(getByte(address)).isEqualTo((byte) -1);

            a.position(101);
            a.mark();
            a.limit(202);

            long address2 = allocateMemory(sizeInBytes, tracker);
            try {
                setMemory(address2, sizeInBytes, (byte) 0);
                initDirectByteBuffer(a, address2, sizeInBytes);
                assertThat(a.hasArray()).isEqualTo(false);
                assertThat(a.isDirect()).isEqualTo(true);
                assertThat(a.capacity()).isEqualTo(sizeInBytes);
                assertThat(a.limit()).isEqualTo(sizeInBytes);
                assertThat(a.position()).isEqualTo(0);
                assertThat(a.remaining()).isEqualTo(sizeInBytes);
                assertThat(getByte(address2)).isEqualTo((byte) 0);
                a.put((byte) -1);
                assertThat(getByte(address2)).isEqualTo((byte) -1);
            } finally {
                free(address2, sizeInBytes, tracker);
            }
        } finally {
            free(address, sizeInBytes, tracker);
        }
    }

    @Test
    void getAddressOfDirectByteBuffer() {
        ByteBuffer buf = ByteBuffer.allocateDirect(8);
        long address = UnsafeUtil.getDirectByteBufferAddress(buf);
        long expected = ThreadLocalRandom.current().nextLong();
        // Disable native access checking, because UnsafeUtil doesn't know about the memory allocation in the
        // ByteBuffer.allocateDirect( … ) call.
        boolean nativeAccessCheckEnabled = UnsafeUtil.exchangeNativeAccessCheckEnabled(false);
        try {
            UnsafeUtil.putLong(address, expected);
            long actual = buf.getLong();
            assertThat(actual).isIn(expected, Long.reverseBytes(expected));
        } finally {
            UnsafeUtil.exchangeNativeAccessCheckEnabled(nativeAccessCheckEnabled);
        }
    }

    @Test
    void closeNativeByteBufferWithUnsafe() {
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(1024);
        assertDoesNotThrow(() -> UnsafeUtil.invokeCleaner(directBuffer));
    }
}
