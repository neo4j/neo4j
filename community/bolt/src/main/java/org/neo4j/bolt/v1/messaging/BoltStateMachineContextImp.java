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
package org.neo4j.bolt.v1.messaging;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.MutableConnectionState;
import org.neo4j.bolt.runtime.StateMachineContext;
import org.neo4j.bolt.runtime.StatementProcessor;
import org.neo4j.bolt.runtime.StatementProcessorProvider;
import org.neo4j.bolt.runtime.TransactionStateMachineSPIProvider;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.bolt.v1.runtime.TransactionStateMachine;

import static java.lang.String.format;
import static org.neo4j.bolt.runtime.StatementProcessor.EMPTY;

public class BoltStateMachineContextImp implements StateMachineContext
{
    private final BoltStateMachine machine;
    private final BoltChannel boltChannel;
    private final BoltStateMachineSPI spi;
    private final MutableConnectionState connectionState;
    private final Clock clock;
    private StatementProcessorProvider statementProcessorProvider;

    public BoltStateMachineContextImp( BoltStateMachine machine, BoltChannel boltChannel, BoltStateMachineSPI spi, MutableConnectionState connectionState,
            Clock clock )
    {
        this.machine = machine;
        this.boltChannel = boltChannel;
        this.spi = spi;
        this.connectionState = connectionState;
        this.clock = clock;
    }

    @Override
    public void authenticatedAsUser( String username, String userAgent )
    {
        boltChannel.updateUser( username, userAgent );
    }

    @Override
    public void handleFailure( Throwable cause, boolean fatal ) throws BoltConnectionFatality
    {
        machine.handleFailure( cause, fatal );
    }

    @Override
    public boolean resetMachine() throws BoltConnectionFatality
    {
        return machine.reset();
    }

    @Override
    public BoltStateMachineSPI boltSpi()
    {
        return spi;
    }

    @Override
    public MutableConnectionState connectionState()
    {
        return connectionState;
    }

    @Override
    public Clock clock()
    {
        return clock;
    }

    @Override
    public String connectionId()
    {
        return machine.id();
    }

    @Override
    public void initStatementProcessorProvider( AuthenticationResult authResult )
    {
        TransactionStateMachineSPIProvider transactionSpiProvider = spi.transactionStateMachineSPIProvider();
        this.statementProcessorProvider = new StatementProcessorProvider( authResult, transactionSpiProvider, clock );
    }

    /**
     * We select the {@link StatementProcessor}, a.k.a. {@link TransactionStateMachine} based on the database name provided here.
     * As {@link MutableConnectionState} claims it holds all states of {@link BoltStateMachine}, we let it holds the current statement processor.
     * We will stick to this {@link StatementProcessor} until a RESET message arrives
     * to reset the current statement process and release all resources it may hold.
     */
    @Override
    public StatementProcessor setCurrentStatementProcessorForDatabase( String databaseName ) throws BoltProtocolBreachFatality, BoltIOException
    {
        if ( isCurrentStatementProcessorNotSet( databaseName ) )
        {
            StatementProcessor statementProcessor = statementProcessorProvider.getStatementProcessor( databaseName );
            connectionState().setStatementProcessor( statementProcessor );
            return statementProcessor;
        }
        else
        {
            return connectionState.getStatementProcessor();
        }
    }

    private boolean isCurrentStatementProcessorNotSet( String databaseName ) throws BoltProtocolBreachFatality
    {
        StatementProcessor currentProcessor = connectionState().getStatementProcessor();
        if ( currentProcessor != EMPTY )
        {
            if ( currentProcessor.databaseName().equals( databaseName ) )
            {
                return false; // already set
            }
            else
            {
                throw new BoltProtocolBreachFatality( format( "Changing database without closing the previous is forbidden. " +
                        "Current database name: '%s', new database name: '%s'.", currentProcessor.databaseName(), databaseName ) );
            }
        }
        return true;
    }
}
