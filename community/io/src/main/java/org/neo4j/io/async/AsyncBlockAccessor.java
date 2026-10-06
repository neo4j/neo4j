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
package org.neo4j.io.async;

import java.io.IOException;

public interface AsyncBlockAccessor extends AutoCloseable {

    AsyncBlockAccessor EMPTY_ASYNC_BLOCK_ACCESSOR = new EmptyAsyncBlockAccessor();

    boolean isAvailable();

    void asyncWrite(int fileDescriptor, long pageRef, long offset, long bufferAddress, int bufferSize)
            throws IOException;

    void asyncRead(int fileDescriptor, long pageRef, long offset, long bufferAddress, int bufferSize)
            throws IOException;

    void asyncVectorWrite(
            int fileDescriptor,
            long offset,
            long[] bufferAddresses,
            int[] bufferSizes,
            int length,
            long[] pagesRefs,
            long[] flushStamps,
            int pagesToFlush)
            throws IOException;

    void asyncVectorRead(int fileDescriptor, long offset, long[] bufferAddress, int[] bufferSize) throws IOException;

    void completeSubmitted();

    default void completeAvailable() {
        completeSubmitted();
    }

    AsyncVectorIOData asyncVectorIOData(long key);

    @Override
    void close();

    class EmptyAsyncBlockAccessor implements AsyncBlockAccessor {

        private EmptyAsyncBlockAccessor() {}

        @Override
        public boolean isAvailable() {
            return false;
        }

        @Override
        public void asyncWrite(int fileDescriptor, long pageRef, long offset, long bufferAddress, int bufferSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void asyncRead(int fileDescriptor, long pageRef, long offset, long bufferAddress, int bufferSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void asyncVectorWrite(
                int fileDescriptor,
                long offset,
                long[] bufferAddresses,
                int[] bufferSizes,
                int length,
                long[] pagesRefs,
                long[] flushStamps,
                int pagesToFlush) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void asyncVectorRead(int fileDescriptor, long offset, long[] bufferAddress, int[] bufferSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void completeSubmitted() {
            throw new UnsupportedOperationException();
        }

        @Override
        public AsyncVectorIOData asyncVectorIOData(long key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close() {}
    }
}
