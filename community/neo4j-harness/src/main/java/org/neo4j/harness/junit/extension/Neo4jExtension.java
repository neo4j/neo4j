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
package org.neo4j.harness.junit.extension;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.neo4j.annotations.api.PublicApi;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.harness.Neo4jBuilders;
import org.neo4j.harness.internal.InProcessNeo4j;

/**
 * Community Neo4j JUnit 5 Extension.
 * Allows easily start neo4j instance for testing purposes with junit 5 with various user-provided options and configurations.
 * Can be registered declaratively with {@link ExtendWith} or programmatically using {@link RegisterExtension}.
 * <p>
 * By default it will try to start neo4j with embedded web server on random ports.
 * In case if more advance configuration is required please use {@link RegisterExtension programmatical extension registration} and configure
 * desired Neo4j behaviour using available options.
 * <p>
 * Please note that neo4j server uses dynamic ports and it is necessary
 * for the test code to use {@link Neo4j#httpURI()} and then {@link java.net.URI#resolve(String)} to create the URIs to be invoked.
 * <p>
 * In case if starting embedded web server is not desirable it can be fully disabled by using {@link Neo4jExtensionBuilder#withDisabledServer()}.
 * <p>
 * Usage example:
 * <pre>
 *  <code>
 *  {@literal @}ExtendWith( Neo4jExtension.class )
 *       class TestExample {
 *              {@literal @}Test
 *               void testExample( Neo4j neo4j, GraphDatabaseService databaseService ) {
 *                   // test code
 *               }
 *       }
 *  </code>
 * </pre>
 * The extension follows the lifecycle of junit 5. If you define the extension with {@link ExtendWith} on the test class or {@link RegisterExtension}
 * on a static field, the neo4j instance will start before any tests are executed, and stop after the last test finishes. If you define the
 * extension with {@link RegisterExtension} on a non-static field, there will be a new instance per test method.
 * <p>
 * Example with per method instances:
 * <pre>
 *  <code>
 *  {@literal @}TestInstance( TestInstance.Lifecycle.PER_METHOD ) // This is default, just there for clarity
 *       class TestExample {
 *             {@literal @}RegisterExtension
 *              Neo4jExtension extension = Neo4jExtension.builder().build();
 *
 *             {@literal @}Test
 *              void testExample( Neo4j neo4j, GraphDatabaseService databaseService ) {
 *                  // test code
 *              }
 *       }
 *  </code>
 * </pre>
 */
@PublicApi
public class Neo4jExtension
        implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback, ParameterResolver {
    private static final String NEO4J_NAMESPACE = "neo4j-extension";
    private static final String PER_METHOD_KEY = "perMethod";
    private static final Namespace NAMESPACE = Namespace.create(NEO4J_NAMESPACE);

    private final Neo4jBuilder builder;

    public static Neo4jExtensionBuilder builder() {
        return new Neo4jExtensionBuilder();
    }

    public Neo4jExtension() {
        this(Neo4jBuilders.newInProcessBuilder());
    }

    protected Neo4jExtension(Neo4jBuilder builder) {
        this.builder = builder;
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        instantiateService(context);
    }

    @Override
    public void afterAll(ExtensionContext context) {
        destroyService(context);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        ExtensionContext.Store store = getStore(context);
        if (store.get(Neo4j.class) == null) {
            // beforeEach is the first method to be called with non-static field and per-method lifecycle
            store.put(PER_METHOD_KEY, true);
            instantiateService(context);
        }
    }

    private static ExtensionContext.Store getStore(ExtensionContext context) {
        return context.getStore(NAMESPACE);
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) throws Exception {
        if (getStore(extensionContext).getOrDefault(PER_METHOD_KEY, Boolean.class, false)) {
            destroyService(extensionContext);
        }
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Class<?> paramType = parameterContext.getParameter().getType();
        return paramType.equals(GraphDatabaseService.class)
                || paramType.equals(Neo4j.class)
                || paramType.equals(DatabaseManagementService.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Class<?> paramType = parameterContext.getParameter().getType();
        return getStore(extensionContext).get(paramType, paramType);
    }

    private void instantiateService(ExtensionContext context) {
        Neo4j neo = builder.build();
        DatabaseManagementService managementService = neo.databaseManagementService();
        GraphDatabaseService service = neo.defaultDatabaseService();
        ExtensionContext.Store store = getStore(context);
        store.put(Neo4j.class, neo);
        store.put(DatabaseManagementService.class, managementService);
        store.put(GraphDatabaseService.class, service);
    }

    private static void destroyService(ExtensionContext context) {
        ExtensionContext.Store store = getStore(context);
        store.remove(GraphDatabaseService.class);
        store.remove(DatabaseManagementService.class);
        InProcessNeo4j controls = store.remove(Neo4j.class, InProcessNeo4j.class);
        controls.close();
    }
}
