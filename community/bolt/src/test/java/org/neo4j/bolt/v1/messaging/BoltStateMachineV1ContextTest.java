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

import io.netty.channel.Channel;
import org.junit.jupiter.api.Test;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.MutableConnectionState;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class BoltStateMachineV1ContextTest
{
    @Test
    void shouldHandleFailure() throws BoltConnectionFatality
    {
        BoltStateMachine machine = mock( BoltStateMachine.class );
        BoltStateMachineV1Context context = newContext( machine, mock( BoltStateMachineSPI.class ) );

        RuntimeException cause = new RuntimeException();
        context.handleFailure( cause, true );

        verify( machine ).handleFailure( cause, true );
    }

    @Test
    void shouldResetMachine() throws BoltConnectionFatality
    {
        BoltStateMachine machine = mock( BoltStateMachine.class );
        BoltStateMachineV1Context context = newContext( machine, mock( BoltStateMachineSPI.class ) );

        context.resetMachine();

        verify( machine ).reset();
    }

    private static BoltStateMachineV1Context newContext( BoltStateMachine machine, BoltStateMachineSPI boltSPI )
    {
        BoltChannel boltChannel = new BoltChannel( "bolt-1", "bolt", mock( Channel.class ) );
        return new BoltStateMachineV1Context( machine, boltChannel, boltSPI, new MutableConnectionState(), Clock.systemUTC() );
    }
}
