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
package org.neo4j.bolt.v1.runtime;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InterruptSignal;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.graphdb.security.AuthorizationExpiredException;

import static org.neo4j.util.Preconditions.checkState;

/**
 * When STREAMING, a result is available as a stream of records.
 * These must be PULLed or DISCARDed before any further statements
 * can be executed.
 */
public class StreamingState implements BoltStateMachineState
{
    private BoltStateMachineState readyState;
    private BoltStateMachineState interruptedState;
    private BoltStateMachineState failedState;

    @Override
    public BoltStateMachineState process( RequestMessage message, StateMachineContext context ) throws BoltConnectionFatality
    {
        assertInitialized();
        if ( message instanceof PullAllMessage )
        {
            return processPullAllMessage( context );
        }
        if ( message instanceof DiscardAllMessage )
        {
            return processDiscardAllMessage( context );
        }
        if ( message instanceof ResetMessage )
        {
            return processResetMessage( context );
        }
        if ( message instanceof InterruptSignal )
        {
            return interruptedState;
        }
        return null;
    }

    @Override
    public String name()
    {
        return "STREAMING";
    }

    public void setReadyState( BoltStateMachineState readyState )
    {
        this.readyState = readyState;
    }

    public void setInterruptedState( BoltStateMachineState interruptedState )
    {
        this.interruptedState = interruptedState;
    }

    public void setFailedState( BoltStateMachineState failedState )
    {
        this.failedState = failedState;
    }

    private BoltStateMachineState processPullAllMessage( StateMachineContext context ) throws BoltConnectionFatality
    {
        return processStreamResultMessage( true, context );
    }

    private BoltStateMachineState processDiscardAllMessage( StateMachineContext context ) throws BoltConnectionFatality
    {
        return processStreamResultMessage( false, context );
    }

    private BoltStateMachineState processResetMessage( StateMachineContext context ) throws BoltConnectionFatality
    {
        boolean success = context.resetMachine();
        return success ? readyState : failedState;
    }

    private BoltStateMachineState processStreamResultMessage( boolean pull, StateMachineContext context ) throws BoltConnectionFatality
    {
        try
        {
            context.connectionState().getStatementProcessor().streamResult( recordStream ->
                    context.connectionState().getResponseHandler().onRecords( recordStream, pull ) );

            return readyState;
        }
        catch ( AuthorizationExpiredException e )
        {
            context.handleFailure( e, true );
            return failedState;
        }
        catch ( Throwable e )
        {
            context.handleFailure( e, false );
            return failedState;
        }
    }

    private void assertInitialized()
    {
        checkState( readyState != null, "Ready state not set" );
        checkState( interruptedState != null, "Interrupted state not set" );
        checkState( failedState != null, "Failed state not set" );
    }
}
