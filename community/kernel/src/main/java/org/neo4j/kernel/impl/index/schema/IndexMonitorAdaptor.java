/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexProvider;

/**
 * Adapts an {@link IndexProvider.Monitor index monitor} to behave like a {@link GBPTree.Monitor gbptree monitor}
 * by relaying the calls together with index information and also duplicate call to delegate {@link GBPTree.Monitor}.
 */
class IndexMonitorAdaptor extends GBPTree.Monitor.Delegate
{
    private final IndexProvider.Monitor indexMonitor;
    private final IndexFiles indexFiles;
    private final IndexDescriptor descriptor;

    IndexMonitorAdaptor( GBPTree.Monitor treeMonitor, IndexProvider.Monitor indexMonitor, IndexFiles indexFiles,
            IndexDescriptor descriptor )
    {
        super( treeMonitor );
        this.indexMonitor = indexMonitor;
        this.indexFiles = indexFiles;
        this.descriptor = descriptor;
    }

    @Override
    public void cleanupRegistered()
    {
        indexMonitor.recoveryCleanupRegistered( indexFiles.getStoreFile(), descriptor );
        super.cleanupRegistered();
    }

    @Override
    public void cleanupStarted()
    {
        indexMonitor.recoveryCleanupStarted( indexFiles.getStoreFile(), descriptor );
        super.cleanupStarted();
    }

    @Override
    public void cleanupFinished( long numberOfPagesVisited, long numberOfTreeNodes, long numberOfCleanedCrashPointers, long durationMillis )
    {
        indexMonitor.recoveryCleanupFinished( indexFiles.getStoreFile(), descriptor,
                numberOfPagesVisited, numberOfTreeNodes, numberOfCleanedCrashPointers, durationMillis );
        super.cleanupFinished( numberOfPagesVisited, numberOfTreeNodes, numberOfCleanedCrashPointers, durationMillis );
    }

    @Override
    public void cleanupClosed()
    {
        indexMonitor.recoveryCleanupClosed( indexFiles.getStoreFile(), descriptor );
        super.cleanupClosed();
    }

    @Override
    public void cleanupFailed( Throwable throwable )
    {
        indexMonitor.recoveryCleanupFailed( indexFiles.getStoreFile(), descriptor, throwable );
        super.cleanupFailed( throwable );
    }
}
