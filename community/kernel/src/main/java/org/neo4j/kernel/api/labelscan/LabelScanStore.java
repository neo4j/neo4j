/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.api.labelscan;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Stores label-->nodes mappings. It receives updates in the form of condensed label->node transaction data
 * and can iterate through all nodes for any given label.
 */
public interface LabelScanStore extends Lifecycle
{
    interface Monitor
    {
        Monitor EMPTY = new Monitor.Adaptor();

        class Adaptor implements Monitor
        {
            @Override
            public void init()
            {   // empty
            }

            @Override
            public void noIndex()
            {   // empty
            }

            @Override
            public void notValidIndex()
            {   // empty
            }

            @Override
            public void rebuilding()
            {   // empty
            }

            @Override
            public void rebuilt( long roughNodeCount )
            {   // empty
            }

            @Override
            public void recoveryCleanupRegistered()
            {   // empty
            }

            @Override
            public void recoveryCleanupStarted()
            {   // empty
            }

            @Override
            public void recoveryCleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis )
            {   // empty
            }

            @Override
            public void recoveryCleanupClosed()
            {   // empty
            }

            @Override
            public void recoveryCleanupFailed( Throwable throwable )
            {   // empty
            }
        }

        void init();

        void noIndex();

        void notValidIndex();

        void rebuilding();

        void rebuilt( long roughNodeCount );

        void recoveryCleanupRegistered();

        void recoveryCleanupStarted();

        void recoveryCleanupFinished( long numberOfPagesVisited, long numberOfCleanedCrashPointers, long durationMillis );

        void recoveryCleanupClosed();

        void recoveryCleanupFailed( Throwable throwable );
    }

    /**
     * From the point a {@link LabelScanReader} is created till it's {@link LabelScanReader#close() closed} the
     * contents it returns cannot change, i.e. it honors repeatable reads.
     *
     * @return a {@link LabelScanReader} capable of retrieving nodes for labels.
     */
    LabelScanReader newReader();

    /**
     * Acquire a writer for updating the store.
     *
     * @return {@link LabelScanWriter} which can modify the {@link LabelScanStore}.
     */
    LabelScanWriter newWriter();

    /**
     * Forces all changes to disk. Called at certain points from within Neo4j for example when
     * rotating the logical log. After completion of this call there cannot be any essential state that
     * hasn't been forced to disk.
     *
     * @throws UnderlyingStorageException if there was a problem forcing the state to persistent storage.
     */
    void force( IOLimiter limiter ) throws UnderlyingStorageException;

    /**
     * Acquire a reader for all {@link NodeLabelRange node label} ranges.
     *
     * @return the {@link AllEntriesLabelScanReader reader}.
     */
    AllEntriesLabelScanReader allNodeLabelRanges();

    ResourceIterator<File> snapshotStoreFiles();

    /**
     * @return {@code true} if there's no data at all in this label scan store, otherwise {@code false}.
     * @throws IOException on I/O error.
     */
    boolean isEmpty() throws IOException;

    /**
     * Initializes the store. After this has been called recovery updates can be processed.
     */
    @Override
    void init() throws IOException;

    /**
     * Starts the store. After this has been called updates can be processed.
     */
    @Override
    void start() throws IOException;

    @Override
    void stop();

    /**
     * Shuts down the store and all resources acquired by it.
     */
    @Override
    void shutdown() throws IOException;

    /**
     * Drops any persistent storage backing this store.
     *
     * @throws IOException on I/O error.
     */
    void drop() throws IOException;

    /**
     * @return whether or not this index is read-only.
     */
    boolean isReadOnly();

    /**
     * @return whether or not there's an existing store present for this label scan store.
     * @throws IOException on I/O error checking the presence of a store.
     */
    boolean hasStore();

    /**
     * Returns the path to label scan store, might be a directory or a file depending on the implementation.
     *
     * @return the directory or file where the label scan store is persisted.
     */
    File getLabelScanStoreFile();
}
