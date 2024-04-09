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
package org.neo4j.internal.batchimport.input;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.KernelVersion.VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralTestDirectoryExtension
class ValueTypeTest {
    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Test
    void arraysShouldCalculateCorrectLength() throws IOException {
        // given
        int[] value = new int[3];
        for (int i = 0; i < value.length; i++) {
            value[i] = 100 + i;
        }
        ValueType valueType = ValueType.typeOf(value);
        CountingChannel channel = new CountingChannel();

        // when
        int length = valueType.length(value);
        valueType.write(value, channel);

        // then
        int expected = 1
                + // component type
                Integer.BYTES
                + // array length
                value.length * Integer.BYTES; // array data
        assertEquals(expected, length);
        assertEquals(expected, channel.position());
    }

    private static class CountingChannel implements FlushableChannel {
        private long position;

        @Override
        public Flushable prepareForFlush() {
            throw new UnsupportedOperationException();
        }

        @Override
        public CountingChannel put(byte value) {
            position += Byte.BYTES;
            return this;
        }

        @Override
        public CountingChannel putShort(short value) {
            position += Short.BYTES;
            return this;
        }

        @Override
        public CountingChannel putInt(int value) {
            position += Integer.BYTES;
            return this;
        }

        @Override
        public CountingChannel putLong(long value) {
            position += Long.BYTES;
            return this;
        }

        @Override
        public CountingChannel putFloat(float value) {
            position += Float.BYTES;
            return this;
        }

        @Override
        public CountingChannel putDouble(double value) {
            position += Double.BYTES;
            return this;
        }

        @Override
        public CountingChannel put(byte[] value, int offset, int length) {
            position += length;
            return this;
        }

        @Override
        public CountingChannel putVersion(byte version) {
            if (KernelVersion.getForVersion(version).isAtLeast(VERSION_ENVELOPED_TRANSACTION_LOGS_INTRODUCED)) {
                // version is not part of the data when writing envelopes
                return this;
            }
            return put(version);
        }

        @Override
        public CountingChannel putAll(ByteBuffer src) {
            position += src.remaining();
            src.position(src.limit()); // Consume buffer
            return this;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {}

        long position() {
            return position;
        }

        @Override
        public int write(ByteBuffer src) {
            int remaining = src.remaining();
            putAll(src);
            return remaining;
        }

        @Override
        public void beginChecksumForWriting() {}

        @Override
        public int putChecksum() {
            position += Integer.BYTES;
            return 0;
        }
    }
}
