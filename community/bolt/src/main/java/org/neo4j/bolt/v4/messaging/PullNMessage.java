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
package org.neo4j.bolt.v4.messaging;

import java.util.Objects;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.StatementMetadata;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PullNMessage implements RequestMessage
{
    public static final byte SIGNATURE = 0x3F;

    private static final String PULL_N_KEY = "n";
    private static final String STATEMENT_ID_KEY = "stmt_id";
    private static final long MINIMAL_N_SIZE = 1;

    private final MapValue meta;
    private final long n;
    private final int statementId;

    public PullNMessage( MapValue meta ) throws BoltIOException
    {
        this.meta = requireNonNull( meta );
        this.n = parseN( meta );
        this.statementId = parseStatementId( meta );
    }

    private long parseN( MapValue meta ) throws BoltIOException
    {
        AnyValue anyValue = meta.get( PULL_N_KEY );
        if ( anyValue != Values.NO_VALUE && anyValue instanceof LongValue )
        {
            long size = ((LongValue) anyValue).longValue();
            if ( size < MINIMAL_N_SIZE )
            {
                throw new BoltIOException( Status.Request.Invalid, format( "Expecting pull size to be at least %s, but got: %s", MINIMAL_N_SIZE, n ) );
            }
            return size;
        }
        else
        {
            throw new BoltIOException( Status.Request.Invalid, format( "Expecting pull size n to be a Long value, but got: %s", anyValue ) );
        }
    }

    private int parseStatementId( MapValue meta )
    {
        AnyValue anyValue = meta.get( STATEMENT_ID_KEY );
        if ( anyValue != Values.NO_VALUE && anyValue instanceof LongValue )
        {
            long id = ((LongValue) anyValue).longValue();
            return Math.toIntExact( id );
        }
        else
        {
            return StatementMetadata.ABSENT_STATEMENT_ID;
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
        PullNMessage that = (PullNMessage) o;
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
        return "PULL_N " + meta;
    }

    public MapValue meta()
    {
        return meta;
    }
}
