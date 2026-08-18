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
package org.neo4j.server.diagnostics;

import java.net.URI;
import java.util.List;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.exceptions.AuthenticationException;
import org.neo4j.kernel.diagnostics.DiagnosticsConnectionException;
import org.neo4j.kernel.diagnostics.DiagnosticsLiveConnection;
import org.neo4j.kernel.diagnostics.DiagnosticsLiveConnectionFactory;
import org.neo4j.kernel.diagnostics.DiagnosticsQueryResult;

/**
 * {@link DiagnosticsLiveConnectionFactory} backed by the Neo4j Java driver (Bolt). Lives in the server module, which
 * already depends on the driver and is part of the {@code neo4j-admin} runtime classpath, so the core modules that
 * only need the diagnostics abstraction do not have to depend on the driver.
 * <p>
 * Connection failures are reported as a {@link DiagnosticsConnectionException} that the diagnostics command turns into
 * a user-facing error.
 */
@ServiceProvider
public class DriverDiagnosticsLiveConnectionFactory implements DiagnosticsLiveConnectionFactory {
    @Override
    public DiagnosticsLiveConnection connect(Config config, String boltUrl, String username, String password) {
        var uri = URI.create(boltUrl != null ? boltUrl : defaultBoltUrl(config));
        var authToken = AuthTokens.basic(username, password);
        var driver = GraphDatabase.driver(uri, authToken);
        try {
            driver.verifyConnectivity();
        } catch (AuthenticationException e) {
            driver.close();
            throw new DiagnosticsConnectionException(
                    "Failed to authenticate against " + uri + ": " + e.getMessage(), e);
        } catch (RuntimeException e) {
            driver.close();
            throw new DiagnosticsConnectionException("Failed to connect to " + uri + ": " + e.getMessage(), e);
        }
        return new BoltDiagnosticsConnection(driver);
    }

    private static String defaultBoltUrl(Config config) {
        // bolt+ssc encrypts but trusts any certificate. The connection is over loopback and exchanges no sensitive
        // data, so skipping certificate validation lets a custom/self-signed server cert work out of the box.
        var scheme = config.get(BoltConnector.encryption_level) == BoltConnector.EncryptionLevel.REQUIRED
                ? "bolt+ssc"
                : "bolt";
        var port = config.get(BoltConnector.listen_address).getPort();
        return String.format("%s://localhost:%d", scheme, port);
    }

    private static final class BoltDiagnosticsConnection implements DiagnosticsLiveConnection {
        private final Driver driver;

        private BoltDiagnosticsConnection(Driver driver) {
            this.driver = driver;
        }

        @Override
        public DiagnosticsQueryResult execute(String database, String query) {
            SessionConfig.Builder sessionConfig = SessionConfig.builder().withDefaultAccessMode(AccessMode.READ);
            if (database != null) {
                sessionConfig.withDatabase(database);
            }
            try (Session session = driver.session(sessionConfig.build())) {
                Result result = session.run(query);
                return new DiagnosticsQueryResult(List.copyOf(result.keys()), result.list(Record::asMap));
            }
        }

        @Override
        public void close() {
            driver.close();
        }
    }
}
