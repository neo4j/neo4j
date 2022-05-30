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

import static org.neo4j.bolt.transport.Neo4jWithSocket.withOptionalBoltEncryption;
import static org.neo4j.values.storable.Values.longValue;

import java.io.IOException;
import java.util.stream.Stream;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.transport.Neo4jWithSocket;
import org.neo4j.fabric.config.FabricSettings;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.AnyValue;

public abstract class AbstractBoltITBase {
    @Inject
    public Neo4jWithSocket server;

    protected TransportConnection connection;
    protected HostnamePort address;

    protected static Stream<TransportConnection.Factory> argumentsProvider() {
        return TransportConnection.factories();
    }

    protected static Condition<AnyValue> longValueCondition(long expected) {
        return new Condition<>(value -> value.equals(longValue(expected)), "equals");
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

    protected void init(TransportConnection.Factory connectionFactory) throws Exception {
        connection = connectionFactory.create(address).connect();
        negotiateBolt();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.disconnect();
        }
    }

    protected abstract void negotiateBolt() throws Exception;

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
