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
import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltDefaultWire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.fabric.config.FabricSettings;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;

public abstract class AbstractBoltITBase {
    @Inject
    public Neo4jWithSocket server;

    protected TransportConnection connection;
    protected HostnamePort address;
    protected BoltWire wire;

    protected static Stream<TransportConnection.Factory> argumentsProvider() {
        return TransportConnection.factories();
    }

    @BeforeEach
    public void setUp(TestInfo testInfo) throws IOException {
        server.setConfigure(settings -> {
            withOptionalBoltEncryption().accept(settings);
            settings.put(FabricSettings.enabled_by_default, false);
        });

        server.init(testInfo);
        address = server.lookupDefaultConnector();
    }

    protected void connect(TransportConnection.Factory connectionFactory) throws IOException {
        connection = connectionFactory.create(address).connect();
        wire = this.initWire();
    }

    protected void connectAndNegotiate(TransportConnection.Factory connectionFactory) throws IOException {
        this.connect(connectionFactory);

        connection.send(this.wire.getProtocolVersion());
        assertThat(connection).negotiates(this.wire.getProtocolVersion());
    }

    protected void connectAndHandshake(TransportConnection.Factory connectionFactory, Feature... features)
            throws Exception {
        this.connectAndNegotiate(connectionFactory);

        wire.enable(features);

        connection.send(wire.hello());
        assertThat(connection).receivesSuccess();
    }

    protected BoltWire initWire() {
        return new BoltDefaultWire();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.disconnect();
        }
        wire = null;
    }

    protected long getLastClosedTransactionId() {
        var resolver = ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver();
        var txIdStore = resolver.resolveDependency(TransactionIdStore.class);
        return txIdStore.getLastClosedTransactionId();
    }

    protected NamedDatabaseId getDatabaseId() {
        var resolver = ((GraphDatabaseAPI) server.graphDatabaseService()).getDependencyResolver();
        var database = resolver.resolveDependency(Database.class);
        return database.getNamedDatabaseId();
    }
}
