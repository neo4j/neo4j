/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.query;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.impl.SystemOutput;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.kernel.impl.query.QueryLoggerIT.readAllLines;

public class ShellQueryLoggingIT
{
    private final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final TestDirectory dir = TestDirectory.testDirectory( getClass() );
    private final DatabaseRule db = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseFactory factory )
        {
            ((TestGraphDatabaseFactory) factory).setFileSystem( fs.get() );
        }

        @Override
        protected void configure( GraphDatabaseBuilder builder )
        {
            builder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE );
            builder.setConfig( GraphDatabaseSettings.logs_directory, logsDirectory().getPath() );
        }
    };
    @Rule
    public final TestRule order = outerRule( dir ).around( fs ).around( db )
            .around( ( base, description ) -> new Statement()
            {
                @Override
                public void evaluate() throws Throwable
                {
                    try
                    {
                        base.evaluate();
                    }
                    catch ( Throwable e )
                    {
                        System.err.println( "<Shell Output>" );
                        System.err.print( out );
                        System.err.println( "</Shell Output>" );
                        throw e;
                    }
                }
            } );
    private final StringWriter out = new StringWriter();
    private GraphDatabaseShellServer server;
    private ShellClient client;

    @Before
    public void setup() throws Exception
    {
        server = new GraphDatabaseShellServer( db.getGraphDatabaseAPI() );
        SystemOutput output = new SystemOutput( new PrintWriter( out ) );
        client = ShellLobby.newClient( server, new HashMap<>(), output, action -> () ->
        {
            // we don't need any handling of CTRL+C
        } );
        out.getBuffer().setLength( 0 ); // clear the output (remove the welcome message)
    }

    @After
    public void shutdown() throws Exception
    {
        client.shutdown();
        server.shutdown();
    }

    @Test
    public void shouldLogQueryWhenExecutingDirectly() throws Exception
    {
        // given
        String query = "CREATE (foo:Foo{bar:'baz'}) RETURN foo.bar";

        // when
        try ( Result result = db.execute( query );
              PrintWriter out = new PrintWriter( this.out ) )
        {
            result.writeAsStringTo( out );
        }

        // then
        assertThat( out.toString(), allOf(
                containsString( "Nodes created: 1" ),
                containsString( "Properties set: 1" ),
                containsString( "Labels added: 1" ) ) );
        assertThat( queryLog(), contains( containsString( query ) ) );
    }

    @Test
    public void shouldLogReadQuery() throws Exception
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "hello", "world" );

            tx.success();
        }

        // then
        shouldLogQuery( "MATCH (n) RETURN n" );
    }

    @Test
    public void shouldLogWriteQuery() throws Exception
    {
        shouldLogQuery( "CREATE (:Foo{bar:'baz'})" );
    }

    private void shouldLogQuery( String query ) throws ShellException, IOException
    {
        // when
        client.evaluate( query + ';' );

        // then
        assertThat( queryLog(), contains( containsString( query ) ) );
    }

    private List<String> queryLog() throws IOException
    {
        return readAllLines( fs.get(), queryLogFile() );
    }

    private File queryLogFile()
    {
        return new File( logsDirectory(), "query.log" );
    }

    private File logsDirectory()
    {
        File logsDir = new File( dir.graphDbDir(), "logs" );
        fs.get().mkdirs( logsDir );
        return logsDir;
    }
}
