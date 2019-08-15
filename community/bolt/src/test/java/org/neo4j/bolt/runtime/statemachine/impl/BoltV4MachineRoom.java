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

import org.mockito.Mockito;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.StatementProcessorReleaseManager;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPI;
import org.neo4j.bolt.runtime.statemachine.TransactionStateMachineSPIProvider;
import org.neo4j.bolt.testing.BoltTestUtil;
import org.neo4j.bolt.v4.BoltStateMachineV4;
import org.neo4j.bolt.v4.messaging.BoltV4Messages;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.BoltMatchers.hasTransaction;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;

/**
 * Helpers for testing the {@link BoltStateMachine}.
 */
public class BoltV4MachineRoom
{
    private BoltV4MachineRoom()
    {
    }

    public static BoltStateMachine newMachine()
    {
        return newMachine( Mockito.mock( BoltStateMachineSPIImpl.class, RETURNS_MOCKS ) );
    }

    public static BoltStateMachine newMachine( BoltStateMachineSPIImpl spi )
    {
        BoltChannel boltChannel = BoltTestUtil.newTestBoltChannel();
        return new BoltStateMachineV4( spi, boltChannel, Clock.systemUTC() );
    }

    public static BoltStateMachine newMachineWithTransaction() throws BoltConnectionFatality, BoltIOException
    {
        BoltStateMachine machine = newMachine();
        init( machine );
        runBegin( machine );
        return machine;
    }

    public static BoltStateMachine newMachineWithTransactionSPI( TransactionStateMachineSPI transactionSPI )
            throws BoltConnectionFatality, BoltIOException
    {
        BoltStateMachineSPI spi = mock( BoltStateMachineSPI.class, RETURNS_MOCKS );
        TransactionStateMachineSPIProvider transactionSPIProvider = mock( TransactionStateMachineSPIProvider.class );
        when( transactionSPIProvider.getTransactionStateMachineSPI( any( String.class ), any( StatementProcessorReleaseManager.class ) ) )
                .thenReturn( transactionSPI );
        when( spi.transactionStateMachineSPIProvider() ).thenReturn( transactionSPIProvider );

        BoltChannel boltChannel = BoltTestUtil.newTestBoltChannel();
        BoltStateMachine machine = new BoltStateMachineV4( spi, boltChannel, Clock.systemUTC() );
        init( machine );
        return machine;
    }

    public static BoltStateMachine init( BoltStateMachine machine ) throws BoltConnectionFatality
    {
        machine.process( BoltV4Messages.hello(), nullResponseHandler() );
        return machine;
    }

    public static void reset( BoltStateMachine machine, BoltResponseHandler handler ) throws BoltConnectionFatality
    {
        machine.interrupt();
        machine.process( BoltV4Messages.reset(), handler );
    }

    private static void runBegin( BoltStateMachine machine ) throws BoltConnectionFatality, BoltIOException
    {
        machine.process( BoltV4Messages.begin(), nullResponseHandler() );
        assertThat( machine, hasTransaction() );
    }

}
