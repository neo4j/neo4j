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
package org.neo4j.bolt.v3.messaging.request;

import java.time.Duration;
import java.util.Objects;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark;
import org.neo4j.bolt.v3.messaging.decoder.StatementMode;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Objects.requireNonNull;

public class BeginMessage implements RequestMessage
{
    private final MapValue meta;
    private final StatementMode mode;
    private final Bookmark bookmark;
    private final Duration txTimeout;

    private static final String MODE_KEY = "mode";
    private static final String TX_TIMEOUT_KEY = "tx_timeout";
    public static final byte SIGNATURE = 0x11;

    public BeginMessage() throws BoltIOException
    {
        this( VirtualValues.EMPTY_MAP );
    }

    public BeginMessage( MapValue meta ) throws BoltIOException
    {
        this.meta = requireNonNull( meta );
        this.bookmark = parseBookmark( meta );
        this.mode = parseStatementMode( meta );
        this.txTimeout = parseTransactionTimeout( meta );
    }

    static Bookmark parseBookmark( MapValue meta ) throws BoltIOException
    {
        try
        {
            return Bookmark.fromParamsOrNull( meta );
        }
        catch ( KernelException e )
        {
            throw new BoltIOException( Status.Request.InvalidFormat, e.getMessage(), e );
        }
    }

    static Duration parseTransactionTimeout( MapValue meta ) throws BoltIOException
    {
        AnyValue anyValue = meta.get( TX_TIMEOUT_KEY );
        if ( anyValue == Values.NO_VALUE )
        {
            return null;
        }
        else if ( anyValue instanceof LongValue )
        {
            return Duration.ofMillis( ((LongValue) anyValue).longValue() );
        }
        else
        {
            throw new BoltIOException( Status.Request.InvalidFormat, "Expecting transaction timeout value to be a Long value, but got: " + anyValue );
        }
    }

    static StatementMode parseStatementMode( MapValue meta ) throws BoltIOException
    {
        AnyValue anyValue = meta.get( MODE_KEY );
        if ( anyValue == Values.NO_VALUE )
        {
            return null;
        }
        else if ( anyValue instanceof TextValue )
        {
            return StatementMode.parseMode( ((TextValue) anyValue).stringValue() );
        }
        else
        {
            throw new BoltIOException( Status.Request.InvalidFormat, "Expecting transaction statement mode value to be a String value, but got: " + anyValue );
        }
    }

    public Bookmark bookmark()
    {
        return this.bookmark;
    }

    public StatementMode mode()
    {
        return this.mode;
    }

    public Duration transactionTimeout()
    {
        return this.txTimeout;
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
        BeginMessage that = (BeginMessage) o;
        return Objects.equals( meta, that.meta ) && Objects.equals( meta, that.meta );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta );
    }

    @Override
    public String toString()
    {
        return "BEGIN " + meta;
    }

    public MapValue meta()
    {
        return meta;
    }
}
