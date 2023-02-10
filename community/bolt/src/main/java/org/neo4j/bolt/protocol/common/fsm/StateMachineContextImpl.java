/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.bolt.protocol.common.fsm;

import java.time.Clock;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.MutableConnectionState;
import org.neo4j.bolt.runtime.BoltConnectionFatality;
import org.neo4j.memory.HeapEstimator;

public class StateMachineContextImpl implements StateMachineContext {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(StateMachineContextImpl.class);

    private final Connection connection;
    private final StateMachine machine;
    private final StateMachineSPI spi;
    private final MutableConnectionState connectionState;
    private final Clock clock;

    public StateMachineContextImpl(
            Connection connection,
            StateMachine machine,
            StateMachineSPI spi,
            MutableConnectionState connectionState,
            Clock clock) {
        this.connection = connection;
        this.machine = machine;
        this.spi = spi;
        this.connectionState = connectionState;
        this.clock = clock;
    }

    @Override
    public String connectionId() {
        return this.connection.id();
    }

    @Override
    public Connection connection() {
        return connection;
    }

    @Override
    public Clock clock() {
        return clock;
    }

    @Override
    public StateMachineSPI boltSpi() {
        return spi;
    }

    @Override
    public MutableConnectionState connectionState() {
        return connectionState;
    }

    @Override
    public void handleFailure(Throwable cause, boolean fatal) throws BoltConnectionFatality {
        // FIXME: Call direction reversal
        machine.handleFailure(cause, fatal);
    }
}
