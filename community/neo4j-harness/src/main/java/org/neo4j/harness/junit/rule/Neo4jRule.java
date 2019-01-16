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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.harness.internal.Neo4jBuilder;
import org.neo4j.harness.internal.Neo4jControls;
import org.neo4j.harness.internal.TestNeo4jBuilders;
import org.neo4j.kernel.extension.ExtensionFactory;

/**
 * Community Neo4j JUnit {@link org.junit.Rule rule}.
 * Allows easily start neo4j instance for testing purposes with various user-provided options and configurations.
 * <p>
 * By default it will try to start neo4j with embedded web server on random ports. Therefore it is necessary
 * for the test code to use {@link #httpURI()} and then {@link java.net.URI#resolve(String)} to create the URIs to be invoked.
 * <p>
 * In case if starting embedded web server is not desirable it can be fully disabled by using {@link #withDisabledServer()} configuration option.
 */
public class Neo4jRule implements TestRule
{
    private Neo4jBuilder builder;
    private Neo4jControls controls;
    private PrintStream dumpLogsOnFailureTarget;

    protected Neo4jRule( Neo4jBuilder builder )
    {
        this.builder = builder;
    }

    public Neo4jRule()
    {
        this( TestNeo4jBuilders.newInProcessBuilder() );
    }

    public Neo4jRule( File workingDirectory )
    {
        this( TestNeo4jBuilders.newInProcessBuilder( workingDirectory ) );
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try ( Neo4jControls sc = controls = builder.build() )
                {
                    try
                    {
                        base.evaluate();
                    }
                    catch ( Throwable t )
                    {
                        if ( dumpLogsOnFailureTarget != null )
                        {
                            sc.printLogs( dumpLogsOnFailureTarget );
                        }

                        throw t;
                    }
                }
            }
        };
    }

    public Neo4jRule withConfig( Setting<?> key, String value )
    {
        builder = builder.withConfig( key, value );
        return this;
    }

    public Neo4jRule withConfig( String key, String value )
    {
        builder = builder.withConfig( key, value );
        return this;
    }

    public Neo4jRule withUnmanagedExtension( String mountPath, Class<?> extension )
    {
        builder = builder.withUnmanagedExtension( mountPath, extension );
        return this;
    }

    public Neo4jRule withUnmanagedExtension( String mountPath, String packageName )
    {
        builder = builder.withUnmanagedExtension( mountPath, packageName );
        return this;
    }

    public Neo4jRule withExtensionFactories( Iterable<ExtensionFactory<?>> extensionFactories )
    {
        builder = builder.withExtensionFactories( extensionFactories );
        return this;
    }

    public Neo4jRule withDisabledServer()
    {
        builder = builder.withDisabledServer();
        return this;
    }

    public Neo4jRule withFixture( File cypherFileOrDirectory )
    {
        builder = builder.withFixture( cypherFileOrDirectory );
        return this;
    }

    public Neo4jRule withFixture( String fixtureStatement )
    {
        builder = builder.withFixture( fixtureStatement );
        return this;
    }

    public Neo4jRule withFixture( Function<GraphDatabaseService,Void> fixtureFunction )
    {
        builder = builder.withFixture( fixtureFunction );
        return this;
    }

    public Neo4jRule copyFrom( File sourceDirectory )
    {
        builder = builder.copyFrom( sourceDirectory );
        return this;
    }

    public Neo4jRule withProcedure( Class<?> procedureClass )
    {
        builder = builder.withProcedure( procedureClass );
        return this;
    }

    public Neo4jRule withFunction( Class<?> functionClass )
    {
        builder = builder.withFunction( functionClass );
        return this;
    }

    public Neo4jRule withAggregationFunction( Class<?> functionClass )
    {
        builder = builder.withAggregationFunction( functionClass );
        return this;
    }

    public Neo4jRule dumpLogsOnFailure( PrintStream out )
    {
        dumpLogsOnFailureTarget = out;
        return this;
    }

    public URI boltURI()
    {
        if ( controls == null )
        {
            throw new IllegalStateException( "Cannot access instance URI before or after the test runs." );
        }
        return controls.boltURI();
    }

    public URI httpURI()
    {
        if ( controls == null )
        {
            throw new IllegalStateException( "Cannot access instance URI before or after the test runs." );
        }
        return controls.httpURI();
    }

    public URI httpsURI()
    {
        if ( controls == null )
        {
            throw new IllegalStateException( "Cannot access instance URI before or after the test runs." );
        }
        return controls.httpsURI().orElseThrow( () -> new IllegalStateException( "HTTPS connector is not configured" ) );
    }

    public GraphDatabaseService getGraphDatabaseService()
    {
        return controls.graph();
    }

    public Configuration getConfig()
    {
        return controls.config();
    }
}
