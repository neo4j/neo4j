/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.shell;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.rmi.RemoteException;

import org.neo4j.bolt.v1.runtime.WorkerFactory;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.shell.impl.AbstractClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.SuppressOutput;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.test.rule.SuppressOutput.suppressAll;


public class StartClientIT extends AbstractShellIT
{
    @Rule
    public SuppressOutput mute = suppressAll();
    private GraphDatabaseShellServer shellServer;

    @Before
    public void startDatabase() throws Exception
    {
        makeServerRemotelyAvailable();
    }

    @Test
    public void givenShellClientWhenOpenFileThenExecuteFileCommands()
    {
        // Given
        // an empty database

        // When
        StartClient.main( new String[]{"-file", getClass().getResource( "/testshell.txt" ).getFile()} );

        // Then
        assertNodeExists( "testshell_foo", "testshell_bar" );
    }

    @Test
    public void givenShellClientWhenReadFromStdinThenExecutePipedCommands() throws IOException
    {
        // Given
        // an empty database

        // When
        InputStream realStdin = System.in;
        try
        {
            System.setIn( new ByteArrayInputStream( "CREATE (n {stdin_foo:'stdin_bar'});".getBytes() ) );
            StartClient.main( new String[]{"-file", "-"} );
        }
        finally
        {
            System.setIn( realStdin );
        }

        // Then
        assertNodeExists( "stdin_foo", "stdin_bar" );
    }

    @Test
    public void mustWarnWhenRunningScriptWithUnterminatedMultilineCommands()
    {
        // Given an empty database and the unterminated-cypher-query.txt file.
        String script = getClass().getResource( "/unterminated-cypher-query.txt" ).getFile();

        // When running the script with -file
        String output = runAndCaptureOutput( new String[]{"-file", script} );

        // Then we should get a warning
        assertThat( output, containsString( AbstractClient.WARN_UNTERMINATED_INPUT ) );
    }

    @Test
    public void mustNotAboutExitingWithUnterminatedCommandWhenItIsNothingButComments()
    {
        // Given an empty database and the unterminated-comment.txt file.
        String script = getClass().getResource( "/unterminated-comment.txt" ).getFile();

        // When running the script with -file
        String output = runAndCaptureOutput( new String[]{"-file", script} );

        // Then we should get a warning
        assertThat( output, not( containsString( AbstractClient.WARN_UNTERMINATED_INPUT ) ) );
    }

    @Test
    public void testShellCloseAfterCommandExecution() throws Exception
    {
        PrintStream out = mock( PrintStream.class );
        PrintStream err = mock( PrintStream.class );
        CtrlCHandler ctrlCHandler = mock( CtrlCHandler.class );
        final GraphDatabaseShellServer databaseShellServer = mock( GraphDatabaseShellServer.class );
        when( databaseShellServer.welcome( any() ) )
                .thenReturn( new Welcome( StringUtils.EMPTY, 1, StringUtils.EMPTY ) );
        when( databaseShellServer.interpretLine( any( Serializable.class ), any( String.class ), any( Output.class ) ) )
                .thenReturn( new Response( StringUtils.EMPTY, Continuation.INPUT_COMPLETE ) );
        StartClient startClient = new StartClient( out, err )
        {
            @Override
            protected GraphDatabaseShellServer getGraphDatabaseShellServer( File path, boolean readOnly,
                    String configFile ) throws RemoteException
            {
                return databaseShellServer;
            }
        };

        // when
        startClient.start( new String[]{"-path", db.getStoreDir().getAbsolutePath(), "-c", "CREATE (n {foo:'bar'});"},
                ctrlCHandler );

        // verify
        verify( databaseShellServer ).shutdown();
    }

    @Test
    public void shouldReportEditionThroughDbInfoApp() throws Exception
    {
        // given
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        CtrlCHandler ctrlCHandler = mock( CtrlCHandler.class );
        StartClient client = new StartClient( new PrintStream( out ), new PrintStream( err ) );

        // when
        client.start( new String[]{"-path", db.getStoreDir().getAbsolutePath() + "testDb", "-c",
                        "dbinfo -g Configuration unsupported.dbms.edition"},
                ctrlCHandler );
        // then
        assertEquals( 0, err.size() );
        assertThat( out.toString(), containsString( "\"unsupported.dbms.edition\": \"community\"" ) );
    }

    @Test
    public void shouldPrintVersionAndExit() throws Exception
    {
        // given
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        CtrlCHandler ctrlCHandler = mock( CtrlCHandler.class );
        StartClient client = new StartClient( new PrintStream( out ), new PrintStream( err ) );

        // when
        client.start( new String[]{"-version"}, ctrlCHandler );

        // then
        assertEquals( 0, err.size() );
        String version = out.toString();
        assertThat( version, startsWith( "Neo4j Community, version " ) );
    }

    @Test
    public void shouldNotStartBolt() throws IOException
    {
        // Given
        AssertableLogProvider log = new AssertableLogProvider();

        // When
        new StartClient( System.out, System.err )
        {
            @Override
            protected GraphDatabaseShellServer getGraphDatabaseShellServer( File path, boolean readOnly,
                    String configFile ) throws RemoteException
            {
                TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory().setUserLogProvider( log );
                shellServer = new GraphDatabaseShellServer( factory, path, readOnly, configFile );
                return shellServer;
            }
        }.start( new String[]{"-c", "RETURN 1;", "-path", db.getStoreDir().getAbsolutePath() + "test-db", "-config",
                getClass().getResource( "/config-with-bolt-connector.conf" ).getFile()}, mock( CtrlCHandler.class ) );
        try
        {
            log.assertNone( inLog( startsWith( WorkerFactory.class.getPackage().getName() ) ).any() );
        }
        finally
        {
            if ( shellServer != null )
            {
                shellServer.shutdown();
            }
        }
    }

    private void assertNodeExists( String property, Object value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Result result = db.execute( "MATCH (n {" + property + ": $value}) RETURN count(n) AS res",
                    singletonMap( "value", value ) );

            long count = (long) single( result ).getOrDefault( "res", 0 );
            assertEquals( 1, count );
            tx.success();
        }
    }

    private String runAndCaptureOutput( String[] arguments )
    {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( buf );
        PrintStream oldOut = System.out;
        System.setOut( out );

        try
        {
            StartClient.main( arguments );
            out.close();
            return buf.toString();
        }
        finally
        {
            System.setOut( oldOut );
        }
    }

}
