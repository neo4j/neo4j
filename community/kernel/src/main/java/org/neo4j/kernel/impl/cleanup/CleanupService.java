/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.cleanup;

import static java.lang.String.format;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.Thunk;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

public abstract class CleanupService extends LifecycleAdapter
{
    /**
     * @param scheduler {@link JobScheduler} to add the cleanup job on.
     * @param logging {@link Logging} where cleanup happens.
     * @param cleanupNecessity {@link Thunk} for deciding what gets registered at this CleanupService and
     * what doesn't. More specifically 
     * @return a new {@link CleanupService}.
     */
    public static CleanupService create( JobScheduler scheduler, Logging logging, Thunk<Boolean> cleanupNecessity )
    {
        // TODO: implement this by using sun.misc.Cleaner, since those is more efficient than PhantomReferences
        return new ReferenceQueueBasedCleanupService( scheduler, logging, new CleanupReferenceQueue( 1000 ),
                cleanupNecessity );
    }

    private final StringLogger logger;

    protected CleanupService( Logging logging )
    {
        this.logger = logging.getMessagesLog( getClass() );
    }

    public abstract <T> ResourceIterator<T> resourceIterator( Iterator<T> iterator, Closeable closeable );

    void cleanup( CleanupReference reference )
    {
        try
        {
            reference.cleanupNow( false );
        }
        catch ( IOException e )
        {
            logger.warn( "Failure autoclosing a resource during collection", e );
        }
    }

    void logLeakedReference( CleanupReference reference )
    {
        logger.warn( format( "Resource not closed: %s", reference.description() ) );
    }
}