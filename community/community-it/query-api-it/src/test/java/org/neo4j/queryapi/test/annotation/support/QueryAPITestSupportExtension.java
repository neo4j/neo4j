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
package org.neo4j.queryapi.test.annotation.support;

import static java.time.Duration.ofSeconds;
import static org.neo4j.queryapi.test.QueryApiTestUtil.resolveDependency;
import static org.neo4j.queryapi.test.QueryApiTestUtil.setupLogging;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ClassTemplateInvocationContext;
import org.junit.jupiter.api.extension.ClassTemplateInvocationContextProvider;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.queryapi.test.QueryAPITestRetryException;
import org.neo4j.queryapi.test.QueryApiTestUtil;
import org.neo4j.queryapi.test.annotation.BoltTransportType;
import org.neo4j.queryapi.test.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.test.procedure.SleepQueryApiTestProcedure;
import org.neo4j.queryapi.test.testclient.QueryAPITestClient;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.tx.TransactionManager;
import org.neo4j.test.extension.ExtensionCallback;

public class QueryAPITestSupportExtension
        implements ClassTemplateInvocationContextProvider,
                ParameterResolver,
                BeforeAllCallback,
                AfterAllCallback,
                TestExecutionExceptionHandler {

    private final List<QueryAPIClassTemplateInvocationContext> invocationContexts;

    QueryAPITestSupportExtension() {
        invocationContexts = new ArrayList<>();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        setupLogging();

        var testClass = context.getRequiredTestClass();
        var annotation = testClass.getAnnotation(QueryAPITestExtension.class);
        var databaseManagementServiceBuilder = annotation.databaseManagementServiceBuilder();
        var constructor = databaseManagementServiceBuilder.getConstructor();
        var extensionMethods = new ArrayList<Method>();
        for (var method : testClass.getMethods()) {
            if (method.isAnnotationPresent(ExtensionCallback.class)) {
                extensionMethods.add(method);
            }
        }

        for (var transportType : annotation.boltTransports()) {
            var builder = constructor.newInstance();
            builder = builder.setConfig(HttpConnector.enabled, true)
                    .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                    .setConfig(
                            BoltConnectorInternalSettings.local_channel_address,
                            context.getDisplayName() + transportType.name())
                    .setConfig(
                            BoltConnector.enable_object_messages_local_connector,
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

            for (var method : extensionMethods) {
                method.setAccessible(true);
                try {
                    method.invoke(null, builder);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(
                            "The method %s  threw an exception.".formatted(method.getName()), e.getCause());
                }
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

            var maybeSleepQueryApiTestProcedure = setupSleepProcedure(annotation, dbms);

            var txManager = resolveDependency(dbms, TransactionManager.class);

            invocationContexts.add(new QueryAPIClassTemplateInvocationContext(
                    transportType,
                    dbms,
                    testClient,
                    txManager,
                    maybeSleepQueryApiTestProcedure
                            .map(SleepQueryApiTestProcedure::controller)
                            .orElse(null)));
        }
    }

    private static Optional<SleepQueryApiTestProcedure> setupSleepProcedure(
            QueryAPITestExtension annotation, DatabaseManagementService dbms) throws ProcedureException {
        if (annotation.sleepProcedureEnabled()) {
            var sleepQueryApiTestProcedure = new SleepQueryApiTestProcedure();
            resolveDependency(dbms, GlobalProcedures.class).register(sleepQueryApiTestProcedure.callableProcedure());
            return Optional.of(sleepQueryApiTestProcedure);
        }
        return Optional.empty();
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

    @Override
    public void handleTestExecutionException(ExtensionContext context, Throwable throwable) throws Throwable {
        var currentException = throwable;

        if (throwable instanceof QueryAPITestRetryException) {
            // The root cause should be used as cause of failure
            currentException = throwable.getCause();
        } else {
            throw throwable;
        }

        var testClass = context.getRequiredTestClass();
        var annotation = testClass.getAnnotation(QueryAPITestExtension.class);

        for (int attempt = 1; attempt < annotation.maxAttempts(); attempt++) {
            try {
                context.getExecutableInvoker()
                        .invoke(context.getRequiredTestMethod(), context.getRequiredTestInstance());
                return;
            } catch (Throwable t) {
                if (t instanceof QueryAPITestRetryException) {
                    currentException = t.getCause();
                } else {
                    throw t;
                }
            }
        }

        throw currentException;
    }
}
