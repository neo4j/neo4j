/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;

class GBPTreeCleanupJob implements CleanupJob
{
    private final CrashGenerationCleaner crashGenerationCleaner;
    private final StampedLock stampedLock;
    private final long stamp;
    private volatile boolean needed;
    private volatile Exception failure;

    /**
     * @param crashGenerationCleaner {@link CrashGenerationCleaner} to use for cleaning.
     * @param lock {@link Lock} to be released when job has either successfully finished or failed.
     */
    GBPTreeCleanupJob( CrashGenerationCleaner crashGenerationCleaner, StampedLock lock, long stamp )
    {
        this.crashGenerationCleaner = crashGenerationCleaner;
        this.stampedLock = lock;
        this.stamp = stamp;
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
    public Exception getCause()
    {
        return failure;
    }

    @Override
    public void run()
    {
        try
        {
            crashGenerationCleaner.clean();
            needed = false;
        }
        catch ( IOException e )
        {
            failure = e;
        }
        finally
        {
            stampedLock.unlockWrite( stamp );
        }
    }
}
