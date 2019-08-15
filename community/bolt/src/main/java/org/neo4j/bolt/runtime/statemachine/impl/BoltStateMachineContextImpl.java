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
package org.neo4j.bolt.runtime.statemachine.impl;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.MutableConnectionState;
import org.neo4j.bolt.runtime.statemachine.StateMachineContext;
import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPIProvider;
import org.neo4j.bolt.security.auth.AuthenticationResult;

import static java.lang.String.format;
import static org.neo4j.bolt.runtime.statemachine.StatementProcessor.EMPTY;

public class BoltStateMachineContextImpl implements StateMachineContext, StatementProcessorReleaseManager
{
    private final BoltStateMachine machine;
    private final BoltChannel boltChannel;
    private final BoltStateMachineSPI spi;
    private final MutableConnectionState connectionState;
    private final Clock clock;
    private StatementProcessorProvider statementProcessorProvider;

    public BoltStateMachineContextImpl( BoltStateMachine machine, BoltChannel boltChannel, BoltStateMachineSPI spi, MutableConnectionState connectionState,
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
        setStatementProcessorProvider( new StatementProcessorProvider( authResult, transactionSpiProvider, clock, this ) );
    }

    /**
     * We select the {@link TransactionStateMachine} based on the database name provided here.
     * This transaction state machine will be kept in {@link MutableConnectionState} until the transaction is closed.
     * When closing, the transaction state machine will perform a callback to {@link #releaseStatementProcessor()} to release itself from {@link
     * MutableConnectionState}.
     * @param databaseName
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
            return connectionState().getStatementProcessor();
        }
    }

    /**
     * This callback is expected to be invoked inside a {@link TransactionStateMachine} to release itself for GC when the transaction is closed.
     * The reason that we have this callback is that the {@link TransactionStateMachine} has a better knowledge of when itself can be released.
     */
    @Override
    public void releaseStatementProcessor()
    {
        connectionState().clearStatementProcessor();
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

    void setStatementProcessorProvider( StatementProcessorProvider statementProcessorProvider )
    {
        this.statementProcessorProvider = statementProcessorProvider;
    }
}
