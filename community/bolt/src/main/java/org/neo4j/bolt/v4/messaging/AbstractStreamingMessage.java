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
package org.neo4j.bolt.v4.messaging;

import java.util.Objects;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.statemachine.StatementMetadata;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.neo4j.bolt.v4.runtime.InTransactionState.QUERY_ID_KEY;

public abstract class AbstractStreamingMessage implements RequestMessage
{
    private static final String STREAM_LIMIT_KEY = "n";
    private static final long STREAM_LIMIT_MINIMAL = 1;
    public static final long STREAM_LIMIT_UNLIMITED = -1;

    private final MapValue meta;
    private final long n;
    private final int statementId;

    public AbstractStreamingMessage( MapValue meta, String name ) throws BoltIOException
    {
        this.meta = requireNonNull( meta );
        this.n = parseN( meta, name );
        this.statementId = parseStatementId( meta );
    }

    private static long parseN( MapValue meta, String name ) throws BoltIOException
    {
        AnyValue anyValue = meta.get( STREAM_LIMIT_KEY );
        if ( anyValue != Values.NO_VALUE && anyValue instanceof LongValue )
        {
            long size = ((LongValue) anyValue).longValue();
            if ( size != STREAM_LIMIT_UNLIMITED && size < STREAM_LIMIT_MINIMAL )
            {
                throw new BoltIOException( Status.Request.Invalid,
                        format( "Expecting %s size to be at least %s, but got: %s", name, STREAM_LIMIT_MINIMAL, size ) );
            }
            return size;
        }
        else
        {
            throw new BoltIOException( Status.Request.Invalid, format( "Expecting %s size n to be a Long value, but got: %s", name, anyValue ) );
        }
    }

    private int parseStatementId( MapValue meta )
    {
        AnyValue anyValue = meta.get( QUERY_ID_KEY );
        if ( anyValue != Values.NO_VALUE && anyValue instanceof LongValue )
        {
            long id = ((LongValue) anyValue).longValue();
            return Math.toIntExact( id );
        }
        else
        {
            return StatementMetadata.ABSENT_QUERY_ID;
        }
    }

    public long n()
    {
        return this.n;
    }

    public int statementId()
    {
        return statementId;
    }

    public MapValue meta()
    {
        return meta;
    }

    @Override
    public boolean safeToProcessInAnyState()
    {
        return false;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        AbstractStreamingMessage that = (AbstractStreamingMessage) o;
        return Objects.equals( meta, that.meta );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta );
    }

    @Override
    public String toString()
    {
        return String.format( "%s %s", name(), meta() );
    }

    abstract String name();
}
