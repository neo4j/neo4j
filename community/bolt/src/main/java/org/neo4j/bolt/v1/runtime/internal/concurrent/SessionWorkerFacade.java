/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal.concurrent;

import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.StatementMetadata;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;

/**
 * A session implementation that delegates work to a worker thread.
 */
public class SessionWorkerFacade implements Session
{
    private final String key;
    private final String connectionDescriptor;
    private final SessionWorker worker;

    public SessionWorkerFacade( String key, String connectionDescriptor, SessionWorker worker )
    {
        this.key = key;
        this.connectionDescriptor = connectionDescriptor;
        this.worker = worker;
    }

    @Override
    public String key()
    {
        return key;
    }

    @Override
    public String connectionDescriptor()
    {
        return connectionDescriptor;
    }

    @Override
    public <A> void init( final String clientName, Map<String,Object> authToken, final A attachment,
            final Callback<Boolean,A> callback )
    {
        queue( session -> session.init( clientName, authToken, attachment, callback ) );
    }

    @Override
    public <A> void run( final String statement, final Map<String,Object> params, final A attachment,
            final Callback<StatementMetadata,A> callback )
    {
        queue( session -> session.run( statement, params, attachment, callback ) );
    }

    @Override
    public <A> void pullAll( final A attachment, final Callback<RecordStream,A> callback )
    {
        queue( session -> session.pullAll( attachment, callback ) );
    }

    @Override
    public <A> void discardAll( final A attachment, final Callback<Void,A> callback )
    {
        queue( session -> session.discardAll( attachment, callback ) );
    }

    @Override
    public <A> void reset( final A attachment, final Callback<Void,A> callback )
    {
        worker.interrupt();
        queue( session -> session.reset( attachment, callback ) );
    }

    @Override
    public <A> void ackFailure( A attachment, Callback<Void,A> callback )
    {
        queue( session -> session.ackFailure( attachment, callback ) );
    }

    @Override
    public void interrupt()
    {
        worker.interrupt();
    }

    @Override
    public void close()
    {
        queue( SessionWorker.SHUTDOWN );
    }

    private void queue( Consumer<Session> action )
    {
        try
        {
            worker.handle( action );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( "Worker interrupted while queueing request, the session may have been " +
                                        "forcibly closed, or the database may be shutting down." );
        }
    }
}
