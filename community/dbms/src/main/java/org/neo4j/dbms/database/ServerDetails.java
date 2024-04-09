/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.dbms.database;

import java.util.Optional;
import java.util.Set;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.systemgraph.InstanceModeConstraint;
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel;

public record ServerDetails(
        ServerId serverId,
        String name,
        Optional<SocketAddress> boltAddress,
        Optional<SocketAddress> httpAddress,
        Optional<SocketAddress> httpsAddress,
        Set<String> tags,
        State state,
        RunningState runningState,
        Set<String> hostedDatabases,
        Set<String> desiredDatabases,
        Set<String> allowedDatabases,
        Set<String> deniedDatabases,
        InstanceModeConstraint modeConstraint,
        Optional<String> neo4jVersion) {
    public enum RunningState {
        /**
         * Present in Discovery Service
         */
        AVAILABLE,
        /**
         * Not present in discovery
         */
        UNAVAILABLE;

        public String prettyPrint() {
            return switch (this) {
                case AVAILABLE -> "Available";
                case UNAVAILABLE -> "Unavailable";
            };
        }
    }

    public enum State {
        /**
         * Server only known via the discovery service (i.e, hasn't been registered with topology graph)
         */
        FREE,

        /**
         * Server has been enabled in the topology graph.
         */
        ENABLED,

        /**
         * Server has been prevented from accepting new allocations.
         */
        CORDONED,

        /**
         * The server is deallocating its databases.
         */
        DEALLOCATING,

        /**
         * The server has deallocated all of its user databases, is in the deallocating state in the topology
         * graph, the system database is either hosted as replica or is rafted,
         * but the member is not voter in the raft group anymore.
         */
        DEALLOCATED,

        /**
         * The server has been dropped from the topology graph.
         */
        DROPPED;

        public static State fromInstanceStatus(TopologyGraphDbmsModel.InstanceStatus status) {
            return switch (status) {
                case ENABLED -> State.ENABLED;
                case CORDONED -> State.CORDONED;
                case DEALLOCATING -> State.DEALLOCATING;
            };
        }

        public String prettyPrint() {
            return switch (this) {
                case FREE -> "Free";
                case ENABLED -> "Enabled";
                case CORDONED -> "Cordoned";
                case DEALLOCATING -> "Deallocating";
                case DEALLOCATED -> "Deallocated";
                case DROPPED -> "Dropped";
            };
        }
    }
}
