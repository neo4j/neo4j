/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.internal;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.graphdb.event.DatabaseEventHandler;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;

/**
 * Handle the collection of kernel event handlers, and fire events as needed.
 */
public class DatabaseEventHandlers extends LifecycleAdapter
{
    private final List<DatabaseEventHandler> databaseEventHandlers = new CopyOnWriteArrayList<>();
    private final Log log;

    public DatabaseEventHandlers( Log log )
    {
        this.log = log;
    }

    @Override
    public void shutdown()
    {
        for ( DatabaseEventHandler databaseEventHandler : databaseEventHandlers )
        {
            databaseEventHandler.beforeShutdown();
        }
    }

    public void registerDatabaseEventHandler( DatabaseEventHandler handler )
    {
        if ( this.databaseEventHandlers.contains( handler ) )
        {
            return;
        }

        // Some algo for putting it in the right place
        for ( DatabaseEventHandler registeredHandler : this.databaseEventHandlers )
        {
            DatabaseEventHandler.ExecutionOrder order = handler.orderComparedTo( registeredHandler );
            int index = this.databaseEventHandlers.indexOf( registeredHandler );
            if ( order == DatabaseEventHandler.ExecutionOrder.BEFORE )
            {
                this.databaseEventHandlers.add( index, handler );
                return;
            }
            else if ( order == DatabaseEventHandler.ExecutionOrder.AFTER )
            {
                this.databaseEventHandlers.add( index + 1, handler );
                return;
            }
        }

        this.databaseEventHandlers.add( handler );
    }

    public void unregisterDatabaseEventHandler( DatabaseEventHandler handler )
    {
        if ( !databaseEventHandlers.remove( handler ) )
        {
            throw new IllegalStateException( handler + " isn't registered" );
        }
    }

    public void kernelPanic( ErrorState error, Throwable cause )
    {
        for ( DatabaseEventHandler handler : databaseEventHandlers )
        {
            try
            {
                handler.panic( error );
            }
            catch ( Throwable e )
            {
                if ( cause != null )
                {
                    e.addSuppressed( cause );
                }
                log.error( "FATAL: Error while handling database panic.", e );
            }
        }
    }
}
