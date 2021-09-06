/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.graphdb.Resource;

/**
 * Represents a "snapshot" of a Neo4j store.
 *
 * The snapshot doesn't contain the bytes of an actual store, of course. It would perhaps be more accurate to say
 * that instances of this class are blueprints from which a true store snapshot can be constructed.
 *
 * In general this works by providing a list of Neo4j store files, along with the latest transaction to be closed/applied
 * against them at the time the snapshot is created. Recipients of a StoreSnapshot object may then construct a copy
 * of the store by retrieving each of the files in the list (see {@code recoverableFiles}), then applying any transactions
 * occurring after the snapshot's latest (i.e. via Recovery).
 *
 * Recovery is required because transactions may have been applied between creating the StoreSnapshot and retrieving the last
 * store file, changing some store files to be logically inconsistent with others.
 *
 * Note: several of the files in a Neo4j store are not updated transactionally, may not be recovered, and must be retrieved in
 * *exactly* the same state as they are in when the StoreSnapshot is created (see {@code unrecoverableFiles}).
 *
 * In an attempt to ensure this, the {@link StoreSnapshot.Factory} implementations claim a lock on the database
 * checkpointer when creating a StoreSnapshot. This lock is released when calling {@link #close()}. The stream
 * of unrecoverable files should be exhausted, and the snapshot closed, as fast as possible.
 */
public final class StoreSnapshot implements AutoCloseable
{
    private final Stream<StoreResource> unrecoverableFiles;
    private final Path[] recoverableFiles;
    private final long lastAppliedTransactionId;
    private final StoreId storeId;
    private final Resource checkPointMutex;

    public StoreSnapshot( Stream<StoreResource> unrecoverableFiles, Path[] recoverableFiles,
            long lastAppliedTransactionId, StoreId storeId, Resource checkPointMutex )
    {
        this.unrecoverableFiles = unrecoverableFiles;
        this.recoverableFiles = recoverableFiles;
        this.lastAppliedTransactionId = lastAppliedTransactionId;
        this.storeId = storeId;
        this.checkPointMutex = checkPointMutex;
    }

    /**
     * @return a stream of store files which are not updated transactionally
     */
    public Stream<StoreResource> unrecoverableFiles()
    {
        return unrecoverableFiles;
    }

    /**
     * @return a collection of the store files which existed at the time this snapshot is created
     */
    public Path[] recoverableFiles()
    {
        return recoverableFiles;
    }

    /**
     * @return the id of the store files
     */
    public StoreId storeId()
    {
        return storeId;
    }

    /**
     * @return the latest transaction id to be closed/applied at the time this snapshot is created
     */
    public long lastAppliedTransactionId()
    {
        return lastAppliedTransactionId;
    }

    @Override
    public void close()
    {
        unrecoverableFiles.close();
        checkPointMutex.close();
    }

    public interface Factory
    {
        /**
         * @return a snapshot of the store files for this database, if available
         */
        Optional<StoreSnapshot> createStoreSnapshot() throws IOException;
    }
}
