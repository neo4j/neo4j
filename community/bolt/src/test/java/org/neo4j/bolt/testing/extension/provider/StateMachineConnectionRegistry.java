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
package org.neo4j.bolt.testing.extension.provider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;

public class StateMachineConnectionRegistry implements ConnectionProvider {
    private final Map<StateMachine, Connection> connectionMap = new HashMap<>();

    public void register(StateMachine fsm, Connection connection) {
        this.connectionMap.put(fsm, connection);
    }

    public List<Connection> getConnections() {
        return new ArrayList<>(this.connectionMap.values());
    }

    @Override
    public Connection forStateMachine(StateMachine fsm) {
        var connection = this.connectionMap.get(fsm);
        if (connection == null) {
            throw new IllegalArgumentException("Cannot provide connection for unknown state machine: " + fsm);
        }

        return connection;
    }
}
