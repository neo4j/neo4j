/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.omg.CORBA.SystemException;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.regex.Pattern.compile;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.asCollection;

public class TestTransactionApps
{
    protected GraphDatabaseAPI db;
    private FakeShellServer shellServer;
    private ShellClient shellClient;

    @Before
    public void doBefore() throws Exception
    {
        db = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();
        shellServer = new FakeShellServer( db );
        shellClient = new SameJvmClient( new HashMap<String, Serializable>(), shellServer, new CollectingOutput() );
   }

    @After
    public void doAfter() throws Exception
    {
        shellClient.shutdown();
        shellServer.shutdown();
        db.shutdown();
    }

    @Test
    public void begin_transaction_opens_a_transaction() throws Exception
    {
        executeCommand( "begin transaction" );
        assertWeAreInATransaction();
    }

    @Test
    public void two_begin_tran_works_as_expected() throws Exception
    {
        executeCommand( "begin tran" );
        executeCommand( "begin transaction" );
        assertWeAreInATransaction();
    }

    @Test
    public void multiple_begins_and_commits_work() throws Exception
    {
        executeCommand( "begin transaction" );
        executeCommand( "begin" );
        executeCommand( "begin transaction" );
        executeCommand( "commit" );
        executeCommand( "commit" );
        executeCommand( "commit" );
        assertWeAreNotInATransaction();
    }

    @Test
    public void commit_tran_closes_open_transaction() throws Exception
    {
        executeCommand( "begin transaction" );
        executeCommand( "commit" );
        assertWeAreNotInATransaction();
    }

    @Test
    public void already_in_transaction() throws Exception
    {
        db.beginTx();
        executeCommand( "begin transaction" );
        executeCommand( "commit" );
        assertWeAreInATransaction();
    }

    @Test
    public void rollback_rolls_everything_back() throws Exception
    {
        db.beginTx();
        executeCommand( "begin transaction" );
        executeCommand( "begin transaction" );
        executeCommand( "begin transaction" );
        executeCommand( "rollback" );
        assertWeAreNotInATransaction();
    }
    @Test
    public void rollback_outside_of_transaction_fails() throws Exception
    {
        executeCommandExpectingException( "rollback", "Not in a transaction" );
    }

    private void assertWeAreNotInATransaction() throws SystemException
    {
        assertTrue( "Expected to not be in a transaction", shellServer.getActiveTransactionCount() == 0 );
    }

    private void assertWeAreInATransaction() throws SystemException
    {

        assertTrue( "Expected to be in a transaction", shellServer.getActiveTransactionCount() > 0 );
    }

    public void executeCommand( String command, String... theseLinesMustExistRegEx ) throws Exception
    {
        executeCommand(shellClient, command, theseLinesMustExistRegEx );
    }

    public void executeCommand( ShellClient client, String command,
                                String... theseLinesMustExistRegEx ) throws Exception
    {
        CollectingOutput output = new CollectingOutput();
        client.evaluate( command, output );

        for ( String lineThatMustExist : theseLinesMustExistRegEx )
        {
            boolean negative = lineThatMustExist.startsWith( "!" );
            lineThatMustExist = negative ? lineThatMustExist.substring( 1 ) : lineThatMustExist;
            Pattern pattern = compile( lineThatMustExist );
            boolean found = false;
            for ( String line : output )
            {
                if ( pattern.matcher( line ).find() )
                {
                    found = true;
                    break;
                }
            }
            assertTrue( "Was expecting a line matching '" + lineThatMustExist + "', but didn't find any from out of " +
                    asCollection( output ), found != negative );
        }
    }

    public void executeCommandExpectingException( String command, String errorMessageShouldContain ) throws Exception
    {
        CollectingOutput output = new CollectingOutput();
        try
        {
            shellClient.evaluate( command, output );
            fail( "Was expecting an exception" );
        } catch ( ShellException e )
        {
            String errorMessage = e.getMessage();
            if ( !errorMessage.toLowerCase().contains( errorMessageShouldContain.toLowerCase() ) )
            {
                fail( "Error message '" + errorMessage + "' should have contained '" + errorMessageShouldContain + "'" );
            }
        }
    }
}

class FakeShellServer extends GraphDatabaseShellServer {

    public FakeShellServer( GraphDatabaseAPI graphDb ) throws RemoteException
    {
        super( graphDb );
    }

    public int getActiveTransactionCount()
    {
        return clients.size();
    }
}
