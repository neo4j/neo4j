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
package org.neo4j.bolt.v4.runtime;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.v3.runtime.FailSafeBoltStateMachineState;
import org.neo4j.bolt.v4.messaging.DiscardResultConsumer;
import org.neo4j.bolt.v4.messaging.DiscardNMessage;
import org.neo4j.bolt.v4.messaging.PullNMessage;
import org.neo4j.bolt.v4.messaging.PullResultConsumer;
import org.neo4j.bolt.v4.messaging.ResultConsumer;

import static org.neo4j.util.Preconditions.checkState;

/**
 * When STREAMING, a result is available as a stream of records.
 * These must be PULLed or DISCARDed before any further statements
 * can be executed.
 */
public abstract class AbstractStreamingState extends FailSafeBoltStateMachineState
{
    protected BoltStateMachineState readyState;

    @Override
    protected BoltStateMachineState processUnsafe( RequestMessage message, StateMachineContext context ) throws Throwable
    {
        if ( message instanceof PullNMessage )
        {
            PullNMessage pullNMessage = (PullNMessage) message;
            return processStreamResultMessage( pullNMessage.statementId(), new PullResultConsumer( context, pullNMessage.n() ), context );
        }
        if ( message instanceof DiscardNMessage )
        {
            DiscardNMessage discardNMessage = (DiscardNMessage) message;
            return processStreamResultMessage( discardNMessage.statementId(), new DiscardResultConsumer( context, discardNMessage.n() ), context );
        }
        return null;
    }

    public void setReadyState( BoltStateMachineState readyState )
    {
        this.readyState = readyState;
    }

    protected abstract BoltStateMachineState processStreamResultMessage( int statementId, ResultConsumer resultConsumer, StateMachineContext context )
            throws Throwable;

    @Override
    protected void assertInitialized()
    {
        checkState( readyState != null, "Ready state not set" );
        super.assertInitialized();
    }
}
