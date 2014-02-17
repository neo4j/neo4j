/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.database;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Entry wrapping one database in the database registry. Manages reference counting and related locking to disable
 * access to the database during shutdown and startup.
 */
class DatabaseRegistryEntry implements Lifecycle
{
    private final Database database;
    private final AtomicInteger referenceCount = new AtomicInteger( -1 );

    public DatabaseRegistryEntry( Database database )
    {
        this.database = database;
    }

    public void visit(DatabaseRegistry.Visitor visitor)
    {
        acquireReference();
        try
        {
            visitor.visit( database );
        }
        finally
        {
            releaseReference();
        }
    }

    private int releaseReference()
    {
        return referenceCount.decrementAndGet();
    }

    private void acquireReference()
    {
        int count;
        do
        {
            count = referenceCount.get();
            if(count == -1)
            {
                throw new RuntimeException( "Database is not currently available, please try again." );
            }
        }
        while(!referenceCount.compareAndSet( count, count + 1 ));
    }

    @Override
    public void init() throws Throwable
    {
        database.init();
    }

    @Override
    public void start() throws Throwable
    {
        database.start();
        enableAccess();
    }

    @Override
    public void stop() throws Throwable
    {
        disableAccess();
        database.stop();
    }

    @Override
    public void shutdown() throws Throwable
    {
        database.shutdown();
    }

    private void enableAccess() throws InterruptedException
    {
        if(!referenceCount.compareAndSet( -1, 0 ))
        {
            throw new IllegalStateException( "Access already enabled!" );
        }
    }

    private void disableAccess() throws InterruptedException
    {
        while(!referenceCount.compareAndSet( 0, -1 ))
        {
            Thread.sleep( 1 );
            if(referenceCount.get() == -1)
            {
                throw new RuntimeException( "Another caller has already blocked access to this database" );
            }
        }
    }
}
