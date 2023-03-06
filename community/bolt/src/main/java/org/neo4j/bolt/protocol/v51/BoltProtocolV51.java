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
package org.neo4j.bolt.protocol.v51;

import java.util.Set;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.AbstractBoltProtocol;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.common.fsm.StateMachineSPI;
import org.neo4j.bolt.protocol.v51.fsm.StateMachineV51;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

public class BoltProtocolV51 extends AbstractBoltProtocol {
    public static final ProtocolVersion VERSION = new ProtocolVersion(5, 1);

    public BoltProtocolV51(SystemNanoClock clock, LogService logging) {
        super(clock, logging);
    }

    @Override
    public ProtocolVersion version() {
        return VERSION;
    }

    @Override
    public Set<Feature> features() {
        return Set.of(Feature.UTC_DATETIME);
    }

    @Override
    protected StateMachine createStateMachine(Connection connection, StateMachineSPI stateMachineSPI) {
        connection.memoryTracker().allocateHeap(StateMachineV51.SHALLOW_SIZE);

        return new StateMachineV51(stateMachineSPI, connection, clock);
    }
}
