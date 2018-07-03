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
package org.neo4j.bolt.v1.runtime;

import java.util.Map;

import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltQuerySource;
import org.neo4j.bolt.runtime.BoltStateMachineState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.runtime.StateMachineMessage;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v1.messaging.Init;
import org.neo4j.values.storable.Values;

import static org.neo4j.kernel.api.security.AuthToken.PRINCIPAL;
import static org.neo4j.util.Preconditions.checkState;

/**
 * Following the socket connection and a small handshake exchange to
 * establish protocol version, the machine begins in the CONNECTED
 * state. The <em>only</em> valid transition from here is through a
 * correctly authorised INIT into the READY state. Any other action
 * results in disconnection.
 */
public class ConnectedState implements BoltStateMachineState
{
    private BoltStateMachineState readyState;
    private BoltStateMachineState failedState;

    @Override
    public BoltStateMachineState process( StateMachineMessage message, StateMachineContext context ) throws BoltConnectionFatality
    {
        assertInitialized();
        if ( message instanceof Init )
        {
            return processInitMessage( (Init) message, context );
        }
        return null;
    }

    @Override
    public String name()
    {
        return "CONNECTED";
    }

    public void setReadyState( BoltStateMachineState readyState )
    {
        this.readyState = readyState;
    }

    public void setFailedState( BoltStateMachineState failedState )
    {
        this.failedState = failedState;
    }

    private BoltStateMachineState processInitMessage( Init message, StateMachineContext context ) throws BoltConnectionFatality
    {
        String userAgent = message.userAgent();
        Map<String,Object> authToken = message.authToken();
        Object principal = authToken.get( PRINCIPAL );
        String owner = String.valueOf( principal );

        try
        {
            AuthenticationResult authResult = context.boltSpi().authenticate( authToken );

            StatementProcessor statementProcessor = newStatementProcessor( owner, userAgent, authResult, context );
            context.connectionState().setStatementProcessor( statementProcessor );

            if ( authResult.credentialsExpired() )
            {
                context.connectionState().onMetadata( "credentials_expired", Values.TRUE );
            }
            context.connectionState().onMetadata( "server", Values.stringValue( context.boltSpi().version() ) );

            context.boltSpi().udcRegisterClient( userAgent );

            if ( principal != null )
            {
                context.connectionState().setOwner( owner );
                context.registerMachine( owner );
            }

            return readyState;
        }
        catch ( Throwable t )
        {
            context.handleFailure( t, true );
            return failedState;
        }
    }

    private static StatementProcessor newStatementProcessor( String owner, String userAgent, AuthenticationResult authResult, StateMachineContext context )
    {
        TransactionStateMachine statementProcessor = new TransactionStateMachine( context.boltSpi().transactionSpi(), authResult, context.clock() );
        statementProcessor.setQuerySource( new BoltQuerySource( owner, userAgent, context.boltSpi().connectionDescriptor() ) );
        return statementProcessor;
    }

    private void assertInitialized()
    {
        checkState( readyState != null, "Ready state not set" );
        checkState( failedState != null, "Failed state not set" );
    }
}
