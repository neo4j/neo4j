/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.runtime.internal.concurrent;

import java.util.Map;

import org.neo4j.function.Consumer;
import org.neo4j.ndp.runtime.Session;
import org.neo4j.ndp.runtime.StatementMetadata;
import org.neo4j.ndp.runtime.spi.RecordStream;

/**
 * A session implementation that delegates work to a worker thread.
 */
public class SessionWorkerFacade implements Session
{
    private final String key;
    private final SessionWorker worker;

    public SessionWorkerFacade( String key, SessionWorker worker )
    {
        this.key = key;
        this.worker = worker;
    }

    @Override
    public String key()
    {
        return key;
    }

    @Override
    public <A> void initialize( final String clientName, final A attachment, final Callback<Void,A> callback )
    {
        queue( new Consumer<Session>()
        {
            @Override
            public void accept( Session session )
            {
                session.initialize( clientName, attachment, callback );
            }
        } );
    }

    @Override
    public <A> void run( final String statement, final Map<String,Object> params, final A attachment,
            final Callback<StatementMetadata,A> callback )
    {
        queue( new Consumer<Session>()
        {
            @Override
            public void accept( Session session )
            {
                session.run( statement, params, attachment, callback );
            }
        } );
    }

    @Override
    public <A> void pullAll( final A attachment, final Callback<RecordStream,A> callback )
    {
        queue( new Consumer<Session>()
        {
            @Override
            public void accept( Session session )
            {
                session.pullAll( attachment, callback );
            }
        } );
    }

    @Override
    public <A> void discardAll( final A attachment, final Callback<Void,A> callback )
    {
        queue( new Consumer<Session>()
        {
            @Override
            public void accept( Session session )
            {
                session.discardAll( attachment, callback );
            }
        } );
    }

    @Override
    public <A> void acknowledgeFailure( final A attachment, final Callback<Void,A> callback )
    {
        queue( new Consumer<Session>()
        {
            @Override
            public void accept( Session session )
            {
                session.acknowledgeFailure( attachment, callback );
            }
        } );
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
