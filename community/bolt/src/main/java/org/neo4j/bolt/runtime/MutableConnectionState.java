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
package org.neo4j.bolt.runtime;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.values.AnyValue;

/**
 * Keeps state of the connection and bolt state machine.
 */
public class MutableConnectionState implements BoltResponseHandler
{
    private Neo4jError pendingError;
    private boolean pendingIgnore;
    private volatile boolean terminated;
    private boolean closed;

    /**
     * Callback poised to receive the next response.
     */
    private BoltResponseHandler responseHandler;
    /**
     * Component responsible for transaction handling and statement execution.
     */
    private StatementProcessor statementProcessor = StatementProcessor.EMPTY;
    /**
     * This is incremented each time {@link BoltStateMachine#interrupt()} is called,
     * and decremented each time a {@code RESET} message
     * arrives. When this is above 0, all messages will be ignored.
     * This way, when a reset message arrives on the network, interrupt
     * can be called to "purge" all the messages ahead of the reset message.
     */
    private final AtomicInteger interruptCounter = new AtomicInteger();

    @Override
    public void onRecords( BoltResult result, boolean pull ) throws Exception
    {
        if ( responseHandler != null )
        {
            responseHandler.onRecords( result, pull );
        }
    }

    @Override
    public void onMetadata( String key, AnyValue value )
    {
        if ( responseHandler != null )
        {
            responseHandler.onMetadata( key, value );
        }
    }

    @Override
    public void markIgnored()
    {
        if ( responseHandler != null )
        {
            responseHandler.markIgnored();
        }
        else
        {
            pendingIgnore = true;
        }
    }

    @Override
    public void markFailed( Neo4jError error )
    {
        if ( responseHandler != null )
        {
            responseHandler.markFailed( error );
        }
        else
        {
            pendingError = error;
        }
    }

    @Override
    public void onFinish()
    {
        if ( responseHandler != null )
        {
            responseHandler.onFinish();
        }
    }

    public Neo4jError getPendingError()
    {
        return pendingError;
    }

    public boolean hasPendingIgnore()
    {
        return pendingIgnore;
    }

    public void resetPendingFailedAndIgnored()
    {
        pendingError = null;
        pendingIgnore = false;
    }

    public boolean canProcessMessage()
    {
        return !closed && pendingError == null && !pendingIgnore;
    }

    public BoltResponseHandler getResponseHandler()
    {
        return responseHandler;
    }

    public void setResponseHandler( BoltResponseHandler responseHandler )
    {
        this.responseHandler = responseHandler;
    }

    public StatementProcessor getStatementProcessor()
    {
        return statementProcessor;
    }

    public void setStatementProcessor( StatementProcessor statementProcessor )
    {
        this.statementProcessor = statementProcessor;
    }

    public boolean isInterrupted()
    {
        return interruptCounter.get() > 0;
    }

    public int incrementInterruptCounter()
    {
        return interruptCounter.incrementAndGet();
    }

    public int decrementInterruptCounter()
    {
        return interruptCounter.decrementAndGet();
    }

    public boolean isTerminated()
    {
        return terminated;
    }

    public void markTerminated()
    {
        terminated = true;
    }

    public boolean isClosed()
    {
        return closed;
    }

    public void markClosed()
    {
        closed = true;
    }
}
