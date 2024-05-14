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
package org.neo4j.cloud.storage.queues;

import static org.neo4j.cloud.storage.StorageUtils.toIOException;
import static org.neo4j.util.Preconditions.requireNonNegative;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.neo4j.function.ThrowingSupplier;

/**
 * A request queue that returns response buffers to the client as soon as they are requested, in the sequential order
 * they are present on cloud storage. The supplier will return <code>null</code> when no more data is remaining.
 */
public abstract class PullQueue extends RequestQueue implements ThrowingSupplier<ByteBuffer, IOException> {

    public static final int QUEUE_SIZE = 4;

    private boolean fillOnNextGet = true;

    /**
     * @param queueSize  the size of the queue that maintains at most <code>queueSize</code> requests concurrently running
     * @param chunkSize  the size of the data chunk to be downloaded in each request
     * @param objectSize the total size of the object in cloud storage
     */
    protected PullQueue(int queueSize, int chunkSize, long objectSize) {
        super(queueSize, chunkSize, objectSize, 0L);
    }

    /**
     * @param position the new position the queue should download content from
     * @return the buffer starting at the requested position or null if no data is available
     * @throws IOException if unable to download the data from the provided position
     */
    public ByteBuffer positionAndGet(long position) throws IOException {
        requireNonNegative(position);
        final var nextRequestPosition = nextRequestPosition();
        if (nextRequestPosition < position) {
            return clearAndPosition(position);
        }

        final var chunkSize = chunkSize();
        var lowestQueueBound = Math.max(0L, nextRequestPosition - ((long) queueSize() * chunkSize));
        if (position < lowestQueueBound) {
            return clearAndPosition(position);
        }

        final var offset = position - lowestQueueBound;
        var chunk = offset / chunkSize;
        final var chunkOffset = (int) (offset % chunkSize);
        while (chunk-- > 0) {
            poll(false).cancel(true);
        }

        final var buffer = get();
        return (buffer == null) ? null : signalFilling(buffer.position(chunkOffset));
    }

    @Override
    public ByteBuffer get() throws IOException {
        if (fillOnNextGet) {
            fillOnNextGet = false;
            fillQueue();
        }
        return asByteBuffer(poll(true));
    }

    private ByteBuffer clearAndPosition(long position) throws IOException {
        clearQueue();
        setNextRequestPosition(position);
        if (!maybeRequestChunk()) {
            return null;
        }

        return signalFilling(asByteBuffer(poll(false)));
    }

    private ByteBuffer asByteBuffer(CompletableFuture<ByteBuffer> response) throws IOException {
        try {
            return response == null ? null : response.get();
        } catch (Exception ex) {
            throw toIOException(ex, () -> "Unable to get the next chunk of data: " + this);
        }
    }

    private ByteBuffer signalFilling(ByteBuffer data) {
        // any calls to get will now fill the queue AFTER this buffer is returned
        // this is handy for random-access style patterns, i.e. hopping around the object via positionAndGet calls
        // ex. a reverse scan of the object would call positionAndGet in decrementing amounts. This could repeatedly
        // call fill with very similar positions, i.e. reloading the same chunks over and over, if the queue was filled
        fillOnNextGet = true;
        return data;
    }
}
