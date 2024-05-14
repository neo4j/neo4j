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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * A request queue that pushes response buffers to the client as soon as they are available, in the sequential order
 * they are present on the cloud storage.
 */
public abstract class PushQueue extends RequestQueue implements Runnable {

    public static final int QUEUE_SIZE = 64;

    /**
     * @param queueSize the size of the queue that maintains at most <code>queueSize</code> requests concurrently running
     * @param chunkSize the size of the data chunk to be downloaded in each request
     * @param objectSize the total size of the object in the cloud storage
     * @param startPosition the initial position of the object in cloud storage
     */
    protected PushQueue(int queueSize, int chunkSize, long objectSize, long startPosition) {
        super(queueSize, chunkSize, objectSize, startPosition);
    }

    /**
     * Callback method for receiving data from some cloud provider. The buffers returned are in the sequential order
     * they are present in the cloud, i.e. the data range 1-1000 would be returned first, followed by 1001-2000, 2001-3000, etc.
     * @param data the data returned from cloud provider
     */
    protected abstract void onData(ByteBuffer data);

    /**
     * Callback method for when an error occurred whilst downloading the cloud data
     * @param ex the exception that occurred
     */
    protected abstract void onError(Exception ex);

    @Override
    public void run() {
        fillQueue();

        try {
            CompletableFuture<ByteBuffer> response;
            // loop while we have some requests still in the queue
            while ((response = poll(true)) != null) {
                // send the data on its merry way
                onData(response.get());
            }
        } catch (Exception ex) {
            onError(toIOException(ex, () -> "Unable to get the next chunk of data: " + this));
        }
    }
}
