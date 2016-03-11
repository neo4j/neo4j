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
package org.neo4j.bolt.v1.runtime.internal;

import java.util.Map;
import java.util.UUID;

import org.neo4j.bolt.v1.runtime.Session;
import org.neo4j.bolt.v1.runtime.StatementMetadata;
import org.neo4j.bolt.v1.runtime.spi.RecordStream;

public class ErrorReportingSession implements Session
{
    private final String connectionDescriptor;
    private final Neo4jError error;
    private final String id;

    public ErrorReportingSession( String connectionDescriptor, Neo4jError error )
    {
        this.error = error;
        this.id = UUID.randomUUID().toString();
        this.connectionDescriptor = connectionDescriptor;
    }

    @Override
    public String key()
    {
        return id;
    }

    @Override
    public String connectionDescriptor()
    {
        return connectionDescriptor;
    }

    private <V, A> void reportError( A attachment, Callback<V,A> callback )
    {
        if ( callback != null )
        {
            callback.failure( error, attachment );
            callback.completed( attachment );
        }
    }

    @Override
    public <A> void init( String clientName, Map<String,Object> authToken, A attachment, Callback<Boolean,A> callback )
    {
        reportError( attachment, callback );
    }

    @Override
    public <A> void run( String statement, Map<String,Object> params, A attachment, Callback<StatementMetadata,A> callback )
    {
        reportError( attachment, callback );
    }

    @Override
    public <A> void pullAll( A attachment, Callback<RecordStream,A> callback )
    {
        reportError( attachment, callback );
    }

    @Override
    public <A> void discardAll( A attachment, Callback<Void,A> callback )
    {
        reportError( attachment, callback );
    }

    @Override
    public <A> void ackFailure( A attachment, Callback<Void,A> callback )
    {
        reportError( attachment, callback );
    }

    @Override
    public <A> void reset( A attachment, Callback<Void,A> callback )
    {
        reportError( attachment, callback );
    }

    @Override
    public void interrupt()
    {

    }

    @Override
    public void close()
    {
    }
}
