/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.bolt.runtime.AccessMode;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.bolt.v3.messaging.request.TransactionInitiatingMessage;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;
import org.neo4j.values.storable.Values;

import static org.neo4j.values.storable.Values.stringArray;
import org.neo4j.memory.HeapEstimator;

/**
 * The READY state indicates that the connection is ready to accept a
 * new RUN request. This is the "normal" state for a connection and
 * becomes available after successful authorisation and when not
 * executing another statement. It is this that ensures that statements
 * must be executed in series and each must wait for the previous
 * statement to complete.
 */
public class ReadyState extends org.neo4j.bolt.v3.runtime.ReadyState
{
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance( ReadyState.class );

    @Override
    public BoltStateMachineState processUnsafe( RequestMessage message, StateMachineContext context ) throws Exception
    {
        if ( message instanceof RunMessage || message instanceof BeginMessage )
        {
            return super.processUnsafe( message, context );
        }
        return null;
    }

    private String extractDatabaseName( TransactionInitiatingMessage message )
    {
        if ( message instanceof RunMessage )
        {
            return ((RunMessage) message).databaseName();
        }
        else if ( message instanceof BeginMessage )
        {
            return ((BeginMessage) message).databaseName();
        }
        else
        {
            throw new IllegalStateException( "Expected either a BoltV4 RUN message or BEGIN message, but got: " + message.getClass() );
        }
    }

    @Override
    protected BoltStateMachineState processRunMessage( org.neo4j.bolt.v3.messaging.request.RunMessage message, StateMachineContext context ) throws Exception
    {
        long start = context.clock().millis();
        var runResult = context.getTransactionManager().runProgram( extractDatabaseName( message ), message.statement(), message.params(),
                                                                    message.bookmarks(), message.getAccessMode().equals( AccessMode.READ ),
                                                                    message.transactionMetadata(), message.transactionTimeout(), context.connectionId() );
        long end = context.clock().millis();

        context.connectionState().setCurrentTransactionId( runResult.transactionId() );

        context.connectionState().onMetadata( FIELDS_KEY, stringArray( runResult.statementMetadata().fieldNames() ) );
        context.connectionState().onMetadata( FIRST_RECORD_AVAILABLE_KEY, Values.longValue( end - start ) );

        return streamingState;
    }

    @Override
    protected BoltStateMachineState processBeginMessage( org.neo4j.bolt.v3.messaging.request.BeginMessage message, StateMachineContext context )
            throws Exception
    {
        String transactionId = context.getTransactionManager().begin( extractDatabaseName( message ), message.bookmarks(),
                                                                      message.getAccessMode().equals( AccessMode.READ ), message.transactionMetadata(),
                                                                      message.transactionTimeout(), context.connectionId() );
        context.connectionState().setCurrentTransactionId( transactionId );
        return txReadyState;
    }
}
