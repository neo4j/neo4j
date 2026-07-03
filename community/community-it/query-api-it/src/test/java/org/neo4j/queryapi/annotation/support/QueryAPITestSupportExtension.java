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
package org.neo4j.queryapi.annotation.support;

import static java.time.Duration.ofSeconds;
import static org.neo4j.queryapi.QueryApiTestUtil.resolveDependency;
import static org.neo4j.queryapi.QueryApiTestUtil.setupLogging;
import static org.neo4j.queryapi.QueryApiTestUtil.sleepProcedure;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ClassTemplateInvocationContext;
import org.junit.jupiter.api.extension.ClassTemplateInvocationContextProvider;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.queryapi.QueryApiTestUtil;
import org.neo4j.queryapi.annotation.BoltTransportType;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.tx.TransactionManager;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class QueryAPITestSupportExtension
        implements ClassTemplateInvocationContextProvider, ParameterResolver, BeforeAllCallback, AfterAllCallback {

    private final List<QueryAPIClassTemplateInvocationContext> invocationContexts;

    QueryAPITestSupportExtension() {
        invocationContexts = new ArrayList<>();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        setupLogging();

        var testClass = context.getRequiredTestClass();
        var annotation = testClass.getAnnotation(QueryAPITestExtension.class);

        for (var transportType : annotation.boltTransports()) {
            var builder = new TestDatabaseManagementServiceBuilder();
            builder = builder.setConfig(HttpConnector.enabled, true)
                    .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                    .setConfig(
                            BoltConnectorInternalSettings.local_channel_address,
                            context.getDisplayName() + transportType.name())
                    .setConfig(
                            BoltConnectorInternalSettings.enable_object_messages_local_connector,
                            transportType == BoltTransportType.LOCAL_CHANNEL_POJO)
                    .setConfig(GraphDatabaseSettings.auth_enabled, annotation.authEnabled())
                    .setConfig(BoltConnector.enabled, true)
                    .setConfig(GraphDatabaseSettings.default_language, GraphDatabaseSettings.CypherVersion.Cypher25)
                    .impermanent();

            if (annotation.queryApiTransactionTimeoutInSeconds() > -1) {
                builder = builder.setConfig(
                        ServerSettings.queryapi_transaction_timeout,
                        ofSeconds(annotation.queryApiTransactionTimeoutInSeconds()));
            }

            if (annotation.transactionTimeoutInSeconds() > -1) {
                builder = builder.setConfig(
                        GraphDatabaseSettings.transaction_timeout, ofSeconds(annotation.transactionTimeoutInSeconds()));
            }

            if (annotation.bookmarkReadyTimeoutInSeconds() > -1) {
                builder = builder.setConfig(
                        GraphDatabaseSettings.bookmark_ready_timeout,
                        Duration.ofSeconds(annotation.bookmarkReadyTimeoutInSeconds()));
            }

            if (annotation.enabledFeatureFlagForUUID()) {
                builder.setConfig(
                        GraphDatabaseInternalSettings.cypher_enable_extra_semantic_features, Set.of("UUIDType"));
            }

            var dbms = builder.build();

            var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
            var queryEndpoint =
                    "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
            var testClient = new QueryAPITestClient(
                    queryEndpoint,
                    annotation.authEnabled() ? "neo4j" : null,
                    annotation.authEnabled() ? "neo4j" : null,
                    annotation.contentType(),
                    Arrays.asList(annotation.acceptedContentTypes()));

            if (annotation.sleepProcedureEnabled()) {
                resolveDependency(dbms, GlobalProcedures.class).register(sleepProcedure());
            }

            var txManager = resolveDependency(dbms, TransactionManager.class);

            invocationContexts.add(
                    new QueryAPIClassTemplateInvocationContext(transportType, dbms, testClient, txManager));
        }
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        for (var invocationContext : invocationContexts) {
            invocationContext.dbms().shutdown();
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return false;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        throw new ParameterResolutionException("This method should never be called");
    }

    @Override
    public boolean supportsClassTemplate(ExtensionContext context) {
        return true;
    }

    @Override
    public Stream<? extends ClassTemplateInvocationContext> provideClassTemplateInvocationContexts(
            ExtensionContext context) {
        return invocationContexts.stream();
    }
}
