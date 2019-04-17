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
package org.neo4j.monitoring;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.event.DefaultDatabaseEvent;
import org.neo4j.logging.Log;

/**
 * Handle the collection of database event listeners, and fire events as needed.
 */
public class DatabaseEventListeners
{
    private final List<DatabaseEventListener> databaseEventListeners = new CopyOnWriteArrayList<>();
    private final Log log;

    public DatabaseEventListeners( Log log )
    {
        this.log = log;
    }

    public void registerDatabaseEventListener( DatabaseEventListener listener )
    {
        if ( databaseEventListeners.contains( listener ) )
        {
            return;
        }
        databaseEventListeners.add( listener );
    }

    public void unregisterDatabaseEventListener( DatabaseEventListener listener )
    {
        if ( !databaseEventListeners.remove( listener ) )
        {
            throw new IllegalStateException( "Database listener `" + listener + "` is not registered." );
        }
    }

    public void databaseStart( String databaseName )
    {
        DefaultDatabaseEvent eventContext = new DefaultDatabaseEvent( databaseName );
        notifyEventListeners( handler -> handler.databaseStart( eventContext ) );
    }

    public void databaseShutdown( String databaseName )
    {
        DefaultDatabaseEvent eventContext = new DefaultDatabaseEvent( databaseName );
        notifyEventListeners( handler -> handler.databaseShutdown( eventContext ) );
    }

    void databasePanic( String databaseName )
    {
        DefaultDatabaseEvent eventContext = new DefaultDatabaseEvent( databaseName );
        notifyEventListeners( handler -> handler.databasePanic( eventContext ) );
    }

    private void notifyEventListeners( Consumer<DatabaseEventListener> consumer )
    {
        for ( DatabaseEventListener listener : databaseEventListeners )
        {
            try
            {
                consumer.accept( listener );
            }
            catch ( Throwable e )
            {
                log.error( "Error while handling database event by listener: " + listener, e );
            }
        }
    }
}
