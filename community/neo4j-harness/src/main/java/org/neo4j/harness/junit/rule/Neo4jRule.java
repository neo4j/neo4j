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
package org.neo4j.harness.junit.rule;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.io.PrintStream;
import java.net.URI;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilder;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserFunction;

import static org.neo4j.harness.Neo4jBuilders.newInProcessBuilder;

/**
 * Community Neo4j JUnit {@link org.junit.Rule rule}.
 * Allows easily start neo4j instance for testing purposes with various user-provided options and configurations.
 * <p>
 * By default it will try to start neo4j with embedded web server on random ports. Therefore it is necessary
 * for the test code to use {@link #httpURI()} and then {@link java.net.URI#resolve(String)} to create the URIs to be invoked.
 * <p>
 * In case if starting embedded web server is not desirable it can be fully disabled by using {@link #withDisabledServer()} configuration option.
 */
@PublicApi
public class Neo4jRule implements TestRule
{
    private Neo4jBuilder builder;
    private Neo4j neo4j;
    private Supplier<PrintStream> dumpLogsOnFailureTarget;

    protected Neo4jRule( Neo4jBuilder builder )
    {
        this.builder = builder;
    }

    public Neo4jRule()
    {
        this( newInProcessBuilder() );
    }

    public Neo4jRule( File workingDirectory )
    {
        this( newInProcessBuilder( workingDirectory ) );
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try ( Neo4j sc = neo4j = builder.build() )
                {
                    try
                    {
                        base.evaluate();
                    }
                    catch ( Throwable t )
                    {
                        if ( dumpLogsOnFailureTarget != null )
                        {
                            sc.printLogs( dumpLogsOnFailureTarget.get() );
                        }

                        throw t;
                    }
                }
            }
        };
    }

    /**
     * Configure the Neo4j instance. Configuration here can be both configuration aimed at the server as well as the
     * database tuning options. Please refer to the Neo4j Manual for details on available configuration options.
     *
     * @param key the config key
     * @param value the config value
     * @param <T> the type of the setting
     * @return this configurator instance
     */
    public <T> Neo4jRule withConfig( Setting<T> key, T value )
    {
        builder = builder.withConfig( key, value );
        return this;
    }

    /**
     * Shortcut for configuring the server to use an unmanaged extension. Please refer to the Neo4j Manual on how to
     * write unmanaged extensions.
     *
     * @param mountPath the http path, relative to the server base URI, that this extension should be mounted at.
     * @param extension the unmanaged extension class.
     * @return this configurator instance
     */
    public Neo4jRule withUnmanagedExtension( String mountPath, Class<?> extension )
    {
        builder = builder.withUnmanagedExtension( mountPath, extension );
        return this;
    }

    /**
     * Shortcut for configuring the server to find and mount all unmanaged extensions in the given package.
     * @see #withUnmanagedExtension(String, Class)
     * @param mountPath the http path, relative to the server base URI, that this extension should be mounted at.
     * @param packageName a java package with extension classes.
     * @return this configurator instance
     */
    public Neo4jRule withUnmanagedExtension( String mountPath, String packageName )
    {
        builder = builder.withUnmanagedExtension( mountPath, packageName );
        return this;
    }

    /**
     * Enhance Neo4j instance with provided extensions.
     * Please refer to the Neo4j Manual for details on extensions, how to write and use them.
     * @param extensionFactories extension factories
     * @return this configurator instance
     */
    public Neo4jRule withExtensionFactories( Iterable<ExtensionFactory<?>> extensionFactories )
    {
        builder = builder.withExtensionFactories( extensionFactories );
        return this;
    }

    /**
     * Disable web server on configured Neo4j instance.
     * For cases where web server is not required to test specific functionality it can be fully disabled using this tuning option.
     * @return this configurator instance.
     */
    public Neo4jRule withDisabledServer()
    {
        builder = builder.withDisabledServer();
        return this;
    }

    /**
     * Data fixtures to inject upon server build. This can be either a file with a plain-text cypher query
     * (for example, myFixture.cyp), or a directory containing such files with the suffix ".cyp".
     * @param cypherFileOrDirectory file with cypher statement, or directory containing ".cyp"-suffixed files.
     * @return this configurator instance
     */
    public Neo4jRule withFixture( File cypherFileOrDirectory )
    {
        builder = builder.withFixture( cypherFileOrDirectory );
        return this;
    }

    /**
     * Data fixture to inject upon server build. This should be a valid Cypher statement.
     * @param fixtureStatement a cypher statement
     * @return this configurator instance
     */
    public Neo4jRule withFixture( String fixtureStatement )
    {
        builder = builder.withFixture( fixtureStatement );
        return this;
    }

    /**
     * Data fixture to inject upon server build. This should be a user implemented fixture function
     * operating on a {@link GraphDatabaseService} instance
     * @param fixtureFunction a fixture function
     * @return this configurator instance
     */
    public Neo4jRule withFixture( Function<GraphDatabaseService,Void> fixtureFunction )
    {
        builder = builder.withFixture( fixtureFunction );
        return this;
    }

    /**
     * Pre-populate the server with databases copied from the specified source directory.
     * The source directory needs to have sub-folders `databases/neo4j` in which the source store files are located.
     * @param sourceDirectory the directory to copy from
     * @return this configurator instance
     */
    public Neo4jRule copyFrom( File sourceDirectory )
    {
        builder = builder.copyFrom( sourceDirectory );
        return this;
    }

    /**
     * Configure the server to load the specified procedure definition class. The class should contain one or more
     * methods annotated with {@link Procedure}, these will become available to call through
     * cypher.
     *
     * @param procedureClass a class containing one or more procedure definitions
     * @return this configurator instance
     */
    public Neo4jRule withProcedure( Class<?> procedureClass )
    {
        builder = builder.withProcedure( procedureClass );
        return this;
    }

    /**
     * Configure the server to load the specified function definition class. The class should contain one or more
     * methods annotated with {@link UserFunction}, these will become available to call through
     * cypher.
     *
     * @param functionClass a class containing one or more function definitions
     * @return this configurator instance
     */
    public Neo4jRule withFunction( Class<?> functionClass )
    {
        builder = builder.withFunction( functionClass );
        return this;
    }

    /**
     * Configure the server to load the specified aggregation function definition class. The class should contain one or more
     * methods annotated with {@link UserAggregationFunction}, these will become available to call through
     * cypher.
     *
     * @param functionClass a class containing one or more function definitions
     * @return this configurator instance
     */
    public Neo4jRule withAggregationFunction( Class<?> functionClass )
    {
        builder = builder.withAggregationFunction( functionClass );
        return this;
    }

    /**
     * Dump available logs on failure.
     * @param out stream used to dump logs into.
     * @return this configurator instance
     */
    public Neo4jRule dumpLogsOnFailure( PrintStream out )
    {
        dumpLogsOnFailureTarget = () -> out;
        return this;
    }

    /**
     * Dump available logs on failure.
     * <p>
     * Similar to {@link #dumpLogsOnFailure(PrintStream)}, but permits late-binding the stream, or producing the stream based on some computation.
     *
     * @param out the supplier of the stream that will be used to dump logs info.
     * @return this configurator instance.
     */
    public Neo4jRule dumpLogsOnFailure( Supplier<PrintStream> out )
    {
        dumpLogsOnFailureTarget = out;
        return this;
    }

    /**
     * Returns the URI to the Bolt Protocol connector of the instance.
     * @return the bolt address.
     */
    public URI boltURI()
    {
        assertInitialised();
        return neo4j.boltURI();
    }

    /**
     * Returns the URI to the root resource of the instance. For example, http://localhost:7474/
     * @return the http address to the root resource.
     */
    public URI httpURI()
    {
        assertInitialised();
        return neo4j.httpURI();
    }

    /**
     * Returns ths URI to the root resource of the instance using the https protocol.
     * For example, https://localhost:7475/.
     * @return the https address to the root resource.
     */
    public URI httpsURI()
    {
        assertInitialised();
        return neo4j.httpsURI();
    }

    /**
     * Access the {@link DatabaseManagementService} used by the server.
     * @return the database management service backing this instance.
     */
    public DatabaseManagementService databaseManagementService()
    {
        assertInitialised();
        return neo4j.databaseManagementService();
    }

    /**
     * Access default database service.
     * @return default database service.
     */
    public GraphDatabaseService defaultDatabaseService()
    {
        assertInitialised();
        return neo4j.defaultDatabaseService();
    }

    /**
     * Returns the server's configuration.
     * @return the current configuration of the instance.
     */
    public Configuration config()
    {
        assertInitialised();
        return neo4j.config();
    }

    private void assertInitialised()
    {
        if ( neo4j == null )
        {
            throw new IllegalStateException( "Cannot access Neo4j before or after the test runs." );
        }
    }
}
