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

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.Logging;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public abstract class CleanupService extends LifecycleAdapter
{
    public static CleanupService create( JobScheduler scheduler, Logging logging )
    {
        // TODO: implement this by using sun.misc.Cleaner, since those is more efficient than PhantomReferences
        return new ReferenceQueueBasedCleanupService( scheduler, logging, new CleanupReferenceQueue( 1000 ) );
    }

    private final StringLogger logger;

    protected CleanupService( Logging logging )
    {
        this.logger = logging.getLogger( getClass() );
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
        logger.warn( String.format( "Resource not closed.", reference ) );
    }
}