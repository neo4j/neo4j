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
package org.neo4j.internal.batchimport.cache.idmapping.string;

import java.util.Arrays;
import org.neo4j.internal.batchimport.cache.ByteArray;

/**
 * {@link Tracker} capable of keeping 6B range values, using {@link ByteArray}.
 */
public class BigIdTracker extends AbstractTracker<ByteArray> {
    static final int SIZE = 5;
    static final int ID_BITS = (Byte.SIZE * SIZE) - 1;
    static final byte[] DEFAULT_VALUE;
    public static final long ID_MASK = (1L << ID_BITS) - 1;

    static {
        DEFAULT_VALUE = new byte[SIZE];
        Arrays.fill(DEFAULT_VALUE, (byte) -1);
    }

    public BigIdTracker(ByteArray array) {
        super(array);
    }

    @Override
    public long get(long index) {
        long value = array.get5ByteLong(index, 0) & ID_MASK;
        return value == ID_MASK ? -1 : value;
    }

    @Override
    public void set(long index, long value) {
        long isDuplicate = array.get5ByteLong(index, 0) & ~ID_MASK;
        array.set5ByteLong(index, 0, isDuplicate | value);
    }

    @Override
    public void markAsDuplicate(long index) {
        array.set5ByteLong(index, 0, array.get5ByteLong(index, 0) & ID_MASK);
    }

    @Override
    public boolean isMarkedAsDuplicate(long index) {
        long isDuplicate = array.get5ByteLong(index, 0) & ~ID_MASK;
        return isDuplicate == 0;
    }
}
