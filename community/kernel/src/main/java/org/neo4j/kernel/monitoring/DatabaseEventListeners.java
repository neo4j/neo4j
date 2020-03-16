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
package org.neo4j.kernel.monitoring;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;

/**
 * Handle the collection of database event listeners, and fire events as needed.
 */
public class DatabaseEventListeners
{
    private final List<DatabaseEventListener> databaseEventListeners = new CopyOnWriteArrayList<>();
    private final List<InternalDatabaseEventListener> internalDatabaseEventListeners = new CopyOnWriteArrayList<>();
    private final Log log;

    public DatabaseEventListeners( Log log )
    {
        this.log = log;
    }

    public void registerDatabaseEventListener( InternalDatabaseEventListener listener )
    {
        addListener( listener, internalDatabaseEventListeners );
    }

    public void unregisterDatabaseEventListener( InternalDatabaseEventListener listener )
    {
        removeListener( listener, internalDatabaseEventListeners );
    }

    public void registerDatabaseEventListener( DatabaseEventListener listener )
    {
        addListener( listener, databaseEventListeners );
    }

    public void unregisterDatabaseEventListener( DatabaseEventListener listener )
    {
        removeListener( listener, databaseEventListeners );
    }

    private <T> void addListener( T listener, List<T> listeners )
    {
        if ( listeners.contains( listener ) )
        {
            return;
        }
        listeners.add( listener );
    }

    private <T> void removeListener( T listener, List<T> listeners )
    {
        if ( !listeners.remove( listener ) )
        {
            throw new IllegalStateException( "Database listener `" + listener + "` is not registered." );
        }
    }

    public void databaseStart( NamedDatabaseId databaseId )
    {
        var event = new StartDatabaseEvent( databaseId );
        notifyEventListeners( handler -> handler.databaseStart( event ), databaseEventListeners );
        notifyEventListeners( handler -> handler.databaseStart( event ), internalDatabaseEventListeners );
    }

    public void databaseShutdown( NamedDatabaseId databaseId )
    {
        var event = new StopDatabaseEvent( databaseId );
        notifyEventListeners( handler -> handler.databaseShutdown( event ), databaseEventListeners );
        notifyEventListeners( handler -> handler.databaseShutdown( event ), internalDatabaseEventListeners );
    }

    void databasePanic( NamedDatabaseId databaseId, Throwable causeOfPanic )
    {
        var event = new PanicDatabaseEvent( databaseId, causeOfPanic );
        notifyEventListeners( handler -> handler.databasePanic( event ), databaseEventListeners );
        notifyEventListeners( handler -> handler.databasePanic( event ), internalDatabaseEventListeners );
    }

    private <T> void notifyEventListeners( Consumer<T> consumer, List<T> listeners )
    {
        for ( var listener : listeners )
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
