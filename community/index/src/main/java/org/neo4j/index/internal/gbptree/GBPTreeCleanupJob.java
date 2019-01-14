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
package org.neo4j.index.internal.gbptree;

import java.io.File;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;

class GBPTreeCleanupJob implements CleanupJob
{
    private final CrashGenerationCleaner crashGenerationCleaner;
    private final GBPTreeLock gbpTreeLock;
    private final GBPTree.Monitor monitor;
    private final File indexFile;
    private volatile boolean needed;
    private volatile Throwable failure;

    /**
     * @param crashGenerationCleaner {@link CrashGenerationCleaner} to use for cleaning.
     * @param gbpTreeLock {@link GBPTreeLock} to be released when job has either successfully finished or failed.
     * @param monitor {@link GBPTree.Monitor} to report to
     * @param indexFile Target file
     */
    GBPTreeCleanupJob( CrashGenerationCleaner crashGenerationCleaner, GBPTreeLock gbpTreeLock, GBPTree.Monitor monitor, File indexFile )
    {
        this.crashGenerationCleaner = crashGenerationCleaner;
        this.gbpTreeLock = gbpTreeLock;
        this.monitor = monitor;
        this.indexFile = indexFile;
        this.needed = true;

    }

    @Override
    public boolean needed()
    {
        return needed;
    }

    @Override
    public boolean hasFailed()
    {
        return failure != null;
    }

    @Override
    public Throwable getCause()
    {
        return failure;
    }

    @Override
    public void close()
    {
        gbpTreeLock.cleanerUnlock();
        monitor.cleanupClosed();
    }

    @Override
    public void run( ExecutorService executor )
    {
        try
        {
            crashGenerationCleaner.clean( executor );
            needed = false;
        }
        catch ( Throwable e )
        {
            monitor.cleanupFailed( e );
            failure = e;
        }
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( ", ", "CleanupJob(", ")" );
        joiner.add( "file=" + indexFile.getAbsolutePath() );
        joiner.add( "needed=" + needed );
        joiner.add( "failure=" + (failure == null ? null : failure.toString()) );
        return joiner.toString();
    }
}
