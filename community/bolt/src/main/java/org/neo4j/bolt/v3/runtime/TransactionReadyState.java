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
package org.neo4j.bolt.v3.runtime;

import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.runtime.StatementMetadata;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.v1.runtime.bookmarking.Bookmark;
import org.neo4j.bolt.v3.messaging.request.CommitMessage;
import org.neo4j.bolt.v3.messaging.request.RollbackMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.values.storable.Values;

import static org.neo4j.bolt.v3.runtime.ReadyState.FIELDS_KEY;
import static org.neo4j.bolt.v3.runtime.ReadyState.FIRST_RECORD_AVAILABLE_KEY;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.values.storable.Values.stringArray;

public class TransactionReadyState extends FailSafeBoltStateMachineState
{
    private BoltStateMachineState streamingState;
    private BoltStateMachineState readyState;

    @Override
    public BoltStateMachineState processUnsafe( RequestMessage message, StateMachineContext context ) throws Exception
    {
        if ( message instanceof RunMessage )
        {
            return processRunMessage( (RunMessage) message, context );
        }
        if ( message instanceof CommitMessage )
        {
            return processCommitMessage( context );
        }
        if ( message instanceof RollbackMessage )
        {
            return processRollbackMessage( context );
        }
        return null;
    }

    @Override
    public String name()
    {
        return "TX_READY";
    }

    public void setTransactionStreamingState( BoltStateMachineState streamingState )
    {
        this.streamingState = streamingState;
    }

    public void setReadyState( BoltStateMachineState readyState )
    {
        this.readyState = readyState;
    }

    private BoltStateMachineState processRunMessage( RunMessage message, StateMachineContext context ) throws KernelException
    {
        long start = context.clock().millis();
        StatementProcessor statementProcessor = context.connectionState().getStatementProcessor();
        StatementMetadata statementMetadata = statementProcessor.run( message.statement(), message.params() );
        long end = context.clock().millis();

        context.connectionState().onMetadata( FIELDS_KEY, stringArray( statementMetadata.fieldNames() ) );
        context.connectionState().onMetadata( FIRST_RECORD_AVAILABLE_KEY, Values.longValue( end - start ) );
        return streamingState;
    }

    private BoltStateMachineState processCommitMessage( StateMachineContext context ) throws Exception
    {
        StatementProcessor statementProcessor = context.connectionState().getStatementProcessor();
        Bookmark bookmark = statementProcessor.commitTransaction();
        bookmark.attachTo( context.connectionState() );
        return readyState;
    }

    private BoltStateMachineState processRollbackMessage( StateMachineContext context ) throws Exception
    {
        StatementProcessor statementProcessor = context.connectionState().getStatementProcessor();
        statementProcessor.rollbackTransaction();
        return readyState;
    }

    @Override
    protected void assertInitialized()
    {
        checkState( streamingState != null, "Streaming state not set" );
        checkState( readyState != null, "Ready state not set" );
        super.assertInitialized();
    }
}
