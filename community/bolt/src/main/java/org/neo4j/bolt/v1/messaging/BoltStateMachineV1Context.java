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
package org.neo4j.bolt.v1.messaging;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineSPI;
import org.neo4j.bolt.runtime.MutableConnectionState;
import org.neo4j.bolt.runtime.StateMachineContext;

public class BoltStateMachineV1Context implements StateMachineContext
{
    private final BoltStateMachine machine;
    private final BoltChannel boltChannel;
    private final BoltStateMachineSPI spi;
    private final MutableConnectionState connectionState;
    private final Clock clock;

    public BoltStateMachineV1Context( BoltStateMachine machine, BoltChannel boltChannel, BoltStateMachineSPI spi,
            MutableConnectionState connectionState, Clock clock )
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
}
