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
package org.neo4j.bolt.v3;

import java.time.Clock;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltStateMachineSPI;
import org.neo4j.bolt.v1.runtime.BoltStateMachineV1;
import org.neo4j.bolt.v1.runtime.ConnectedState;
import org.neo4j.bolt.v1.runtime.FailedState;
import org.neo4j.bolt.v1.runtime.InterruptedState;
import org.neo4j.bolt.v1.runtime.ReadyState;
import org.neo4j.bolt.v1.runtime.StreamingState;
import org.neo4j.bolt.v3.runtime.ExtraMetaDataConnectedState;

public class BoltStateMachineV3 extends BoltStateMachineV1
{
    public BoltStateMachineV3( BoltStateMachineSPI boltSPI, BoltChannel boltChannel, Clock clock )
    {
        super( boltSPI, boltChannel, clock );
    }

    @Override
    protected States buildStates()
    {
        ConnectedState connected = new ExtraMetaDataConnectedState();
        ReadyState ready = new ReadyState();
        StreamingState streaming = new StreamingState();
        FailedState failed = new FailedState();
        InterruptedState interrupted = new InterruptedState();

        connected.setReadyState( ready );
        connected.setFailedState( failed );

        ready.setStreamingState( streaming );
        ready.setInterruptedState( interrupted );
        ready.setFailedState( failed );

        streaming.setReadyState( ready );
        streaming.setInterruptedState( interrupted );
        streaming.setFailedState( failed );

        failed.setReadyState( ready );
        failed.setInterruptedState( interrupted );

        interrupted.setReadyState( ready );
        interrupted.setFailedState( failed );

        return new States( connected, failed );
    }
}
