/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server;

import org.apache.commons.lang.NotImplementedException;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.shell.Output;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.impl.AbstractServer;

import java.io.IOException;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import static org.junit.Assert.assertEquals;

public class ShellTest
{
    @Test
    public void shouldStartNeoShell() throws Exception
    {
        NeoServer server = ServerBuilder.server().withRandomDatabaseDir().withPassingStartupHealthcheck().build();
        server.start();

        setPropertyonReferenceNode( server, "apa", "thisIsJustATest" );


        ShellClient client = ShellLobby.newClient( 1337, AbstractServer.DEFAULT_NAME );

        String actual = getReferencePropertyThroughShell( client );

        assertEquals( "*apa =[thisIsJustATest]", actual );
    }

    private String getReferencePropertyThroughShell( ShellClient client ) throws Exception
    {
        TestOutput testOutput = new TestOutput();

        client.getServer().interpretLine( "ls", client.session(), testOutput );

        return testOutput.result.toString();
    }

    private void setPropertyonReferenceNode( NeoServer server, String key, String value )
    {
        AbstractGraphDatabase graph = server.getDatabase().graph;
        Transaction tx = graph.beginTx();
        graph.getReferenceNode().setProperty( key, value );
        tx.success();
        tx.finish();
    }

    private static class TestOutput extends UnicastRemoteObject implements Output, Serializable
    {
        public Serializable result;

        TestOutput() throws Exception
        {
            super();
        }

        @Override
        public void print( Serializable object ) throws RemoteException
        {
            throw new NotImplementedException();
        }

        @Override
        public void println() throws RemoteException
        {
            throw new NotImplementedException();
        }

        @Override
        public void println( Serializable object ) throws RemoteException
        {
            result = object;
        }

        @Override
        public Appendable append( CharSequence charSequence ) throws IOException
        {
            throw new NotImplementedException();
        }

        @Override
        public Appendable append( CharSequence charSequence, int i, int i1 ) throws IOException
        {
            throw new NotImplementedException();
        }

        @Override
        public Appendable append( char c ) throws IOException
        {
            throw new NotImplementedException();
        }
    }
}
