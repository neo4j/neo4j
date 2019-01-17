/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.harness.junit.extension;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.junit.jupiter.api.extension.RegisterExtension;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.harness.internal.InProcessNeo4j;
import org.neo4j.harness.internal.Neo4jBuilder;
import org.neo4j.harness.junit.Neo4j;

import static org.neo4j.harness.internal.TestNeo4jBuilders.newInProcessBuilder;

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
 *    {@literal @}ExtendWith( Neo4jExtension.class )
 *     class TestExample {
 *            {@literal @}Test
 *             void testExample( Neo4j neo4j, GraphDatabaseService databaseService )
 *             {
 *                 // test code
 *             }
 *    }
 *
 *  </code>
 * </pre>
 */
public class Neo4jExtension implements BeforeAllCallback, AfterAllCallback, ParameterResolver
{
    private static final String NEO4J_NAMESPACE = "neo4j-extension";
    private static final Namespace NAMESPACE = Namespace.create( NEO4J_NAMESPACE );

    private Neo4jBuilder builder;

    public static Neo4jExtensionBuilder builder()
    {
        return new Neo4jExtensionBuilder();
    }

    public Neo4jExtension()
    {
        this( newInProcessBuilder() );
    }

    protected Neo4jExtension( Neo4jBuilder builder )
    {
        this.builder = builder;
    }

    @Override
    public void beforeAll( ExtensionContext context )
    {
        InProcessNeo4j neo4J = builder.build();
        GraphDatabaseService service = neo4J.graph();
        context.getStore( NAMESPACE ).put( Neo4j.class, neo4J );
        context.getStore( NAMESPACE ).put( GraphDatabaseService.class, service );
    }

    @Override
    public void afterAll( ExtensionContext context )
    {
        ExtensionContext.Store store = context.getStore( NAMESPACE );
        store.remove( GraphDatabaseService.class );
        InProcessNeo4j controls = store.remove( Neo4j.class, InProcessNeo4j.class );
        controls.close();
    }

    @Override
    public boolean supportsParameter( ParameterContext parameterContext, ExtensionContext extensionContext ) throws ParameterResolutionException
    {
        Class<?> paramType = parameterContext.getParameter().getType();
        return paramType.equals( GraphDatabaseService.class ) || paramType.equals( Neo4j.class );
    }

    @Override
    public Object resolveParameter( ParameterContext parameterContext, ExtensionContext extensionContext ) throws ParameterResolutionException
    {
        Class<?> paramType = parameterContext.getParameter().getType();
        return extensionContext.getStore( NAMESPACE ).get( paramType, paramType );
    }
}
