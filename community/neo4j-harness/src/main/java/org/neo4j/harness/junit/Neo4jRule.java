/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.harness.junit;

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
import org.neo4j.harness.ServerControls;
import org.neo4j.harness.TestServerBuilder;
import org.neo4j.harness.TestServerBuilders;

/**
 * A convenience wrapper around {@link org.neo4j.harness.TestServerBuilder}, exposing it as a JUnit
 * {@link org.junit.Rule rule}.
 *
 * Note that it will try to start the web server on the standard 7474 port, but if that is not available
 * (typically because you already have an instance of Neo4j running) it will try other ports. Therefore it is necessary
 * for the test code to use {@link #httpURI()} and then {@link java.net.URI#resolve(String)} to create the URIs to be invoked.
 */
public class Neo4jRule implements TestRule, TestServerBuilder
{
    private TestServerBuilder builder;
    private ServerControls controls;
    private PrintStream dumpLogsOnFailureTarget;

    Neo4jRule( TestServerBuilder builder )
    {
        this.builder = builder;
    }

    public Neo4jRule( )
    {
        this( TestServerBuilders.newInProcessBuilder() );
    }

    public Neo4jRule( File workingDirectory )
    {
        this( TestServerBuilders.newInProcessBuilder( workingDirectory ) );
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try ( ServerControls sc = controls = builder.newServer() )
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

    @Override
    public ServerControls newServer()
    {
        throw new UnsupportedOperationException( "The server cannot be manually started via this class, it must be used as a JUnit rule." );
    }

    @Override
    public Neo4jRule withConfig( Setting<?> key, String value )
    {
        builder = builder.withConfig( key, value );
        return this;
    }

    @Override
    public Neo4jRule withConfig( String key, String value )
    {
        builder = builder.withConfig( key, value );
        return this;
    }

    @Override
    public Neo4jRule withExtension( String mountPath, Class<?> extension )
    {
        builder = builder.withExtension( mountPath, extension );
        return this;
    }

    @Override
    public Neo4jRule withExtension( String mountPath, String packageName )
    {
        builder = builder.withExtension( mountPath, packageName );
        return this;
    }

    @Override
    public Neo4jRule withFixture( File cypherFileOrDirectory )
    {
        builder = builder.withFixture( cypherFileOrDirectory );
        return this;
    }

    @Override
    public Neo4jRule withFixture( String fixtureStatement )
    {
        builder = builder.withFixture( fixtureStatement );
        return this;
    }

    @Override
    public Neo4jRule withFixture( Function<GraphDatabaseService, Void> fixtureFunction )
    {
        builder = builder.withFixture( fixtureFunction );
        return this;
    }

    @Override
    public Neo4jRule copyFrom( File sourceDirectory )
    {
        builder = builder.copyFrom( sourceDirectory );
        return this;
    }

    @Override
    public Neo4jRule withProcedure( Class<?> procedureClass )
    {
        builder = builder.withProcedure( procedureClass );
        return this;
    }

    @Override
    public Neo4jRule withFunction( Class<?> functionClass )
    {
        builder = builder.withFunction( functionClass );
        return this;
    }

    @Override
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
