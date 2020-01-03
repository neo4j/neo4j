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
package org.neo4j.bolt.v4.runtime;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.v3.messaging.request.TransactionInitiatingMessage;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;

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
    @Override
    public BoltStateMachineState processUnsafe( RequestMessage message, StateMachineContext context ) throws Exception
    {
        if ( message instanceof RunMessage || message instanceof BeginMessage )
        {
            return super.processUnsafe( message, context );
        }
        return null;
    }

    @Override
    protected StatementProcessor getStatementProcessor( TransactionInitiatingMessage message, StateMachineContext context )
            throws BoltProtocolBreachFatality, BoltIOException
    {
        String databaseName;
        if ( message instanceof RunMessage )
        {
            databaseName = ((RunMessage) message).databaseName();
        }
        else if ( message instanceof BeginMessage )
        {
            databaseName = ((BeginMessage) message).databaseName();
        }
        else
        {
            throw new IllegalStateException( "Expected either a BoltV4 RUN message or BEGIN message, but got: " + message.getClass() );
        }

        return context.setCurrentStatementProcessorForDatabase( databaseName );
    }
}
