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
package org.neo4j.bolt;

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.hello;
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.internal.helpers.HostnamePort;

public abstract class AbstractBoltTransportsTest {

    public TransportConnection.Factory connectionFactory;

    protected HostnamePort address;
    protected TransportConnection connection;

    protected void initParameters(TransportConnection.Factory connectionFactory) throws Exception {
        this.connectionFactory = connectionFactory;

        connection = newConnection();
    }

    protected void initConnection(TransportConnection.Factory factory) throws Exception {
        this.initParameters(factory);

        this.connection.connect().sendDefaultProtocolVersion().send(hello());

        assertThat(connection).negotiatesDefaultVersion().receivesSuccess();
    }

    @AfterEach
    public void disconnectFromDatabase() throws Exception {
        if (connection != null) {
            connection.disconnect();
        }
    }

    protected static Stream<TransportConnection.Factory> argumentsProvider() {
        return TransportConnection.factories();
    }

    protected TransportConnection newConnection() throws Exception {
        return connectionFactory.create(this.address);
    }

    protected void reconnect() throws Exception {
        if (connection != null) {
            connection.disconnect();
        }
        connection = newConnection();
    }

    protected Consumer<Map<Setting<?>, Object>> getSettingsFunction() {
        return withOptionalBoltEncryption();
    }
}
