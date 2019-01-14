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
package org.neo4j.bolt.v1.runtime;

import java.time.Clock;
import java.util.Collections;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.security.auth.AuthenticationException;
import org.neo4j.bolt.security.auth.AuthenticationResult;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.emptyMap;
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
public class MachineRoom
{
    static final MapValue EMPTY_PARAMS = VirtualValues.EMPTY_MAP;
    static final String USER_AGENT = "BoltStateMachineTest/0.0";

    private MachineRoom()
    {
    }

    public static BoltStateMachine newMachine()
    {
        return newMachine( mock( BoltStateMachineSPI.class, RETURNS_MOCKS ) );
    }

    public static BoltStateMachine newMachine( BoltStateMachineSPI spi )
    {
        BoltChannel boltChannel = mock( BoltChannel.class );
        return new BoltStateMachine( spi, boltChannel, Clock.systemUTC(), NullLogService.getInstance() );
    }

    public static BoltStateMachine newMachine( BoltStateMachine.State state ) throws AuthenticationException, BoltConnectionFatality
    {
        BoltStateMachine machine = newMachine();
        init( machine );
        machine.state = state;
        return machine;
    }

    public static BoltStateMachine newMachineWithOwner( BoltStateMachine.State state, String owner ) throws AuthenticationException, BoltConnectionFatality
    {
        BoltStateMachine machine = newMachine();
        init( machine, owner );
        machine.state = state;
        return machine;
    }

    public static BoltStateMachine newMachineWithTransaction( BoltStateMachine.State state )
            throws AuthenticationException, BoltConnectionFatality
    {
        BoltStateMachine machine = newMachine();
        init( machine );
        runBegin( machine );
        machine.state = state;
        return machine;
    }

    public static BoltStateMachine newMachineWithTransactionSPI( TransactionStateMachine.SPI transactionSPI ) throws
            AuthenticationException, BoltConnectionFatality
    {
        BoltStateMachine.SPI spi = mock( BoltStateMachine.SPI.class, RETURNS_MOCKS );
        when( spi.transactionSpi() ).thenReturn( transactionSPI );

        BoltChannel boltChannel = mock( BoltChannel.class );
        BoltStateMachine machine = new BoltStateMachine( spi, boltChannel, Clock.systemUTC(), NullLogService.getInstance() );
        init( machine );
        return machine;
    }

    public static BoltStateMachine init( BoltStateMachine machine ) throws AuthenticationException, BoltConnectionFatality
    {
        return init( machine, null );
    }

    private static BoltStateMachine init( BoltStateMachine machine, String owner ) throws AuthenticationException, BoltConnectionFatality
    {
        AuthenticationResult authenticationResult = mock( AuthenticationResult.class );
        when( machine.spi.authenticate( any() ) ).thenReturn( authenticationResult );
        machine.init( USER_AGENT, owner == null ? emptyMap() : Collections.singletonMap( AuthToken.PRINCIPAL, owner ), nullResponseHandler() );
        return machine;
    }

    private static void runBegin( BoltStateMachine machine ) throws BoltConnectionFatality
    {
        machine.run( "BEGIN", EMPTY_PARAMS, nullResponseHandler() );
        machine.discardAll( nullResponseHandler() );
        assertThat( machine, hasTransaction() );
    }

}
