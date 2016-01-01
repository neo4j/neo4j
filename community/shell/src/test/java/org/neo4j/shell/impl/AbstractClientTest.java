/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.shell.impl;

import org.junit.Test;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Response;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class AbstractClientTest
{

    @Test
    public void shouldHandleNormalInput() throws ShellException, RemoteException
    {
        final CollectingOutput output = new CollectingOutput();
        final String message = "Test method called";
        AbstractClient client = new AbstractClient( null, null )
        {
            @Override
            public ShellServer getServer()
            {
                ShellServer server = null;
                try
                {
                    server = new GraphDatabaseShellServer( null )
                    {

                        @Override
                        public Response interpretLine( Serializable clientId, String line, Output out )
                                throws ShellException
                        {
                            try
                            {
                                out.println( message );
                            }
                            catch ( RemoteException e )
                            {
                            }
                            return new Response( "", Continuation.INPUT_COMPLETE );
                        }
                    };
                }
                catch ( RemoteException e )
                {
                }
                return server;
            }

            @Override
            public Output getOutput()
            {
                return output;
            }
        };

        client.evaluate( "RETURN 1;" );

        Set<String> messages = new HashSet<>();
        for ( String s : output )
        {
            messages.add( s );
        }
        assertThat( messages, contains( message ) );
    }

    @Test
    public void shouldExitMultilineModeAfterGettingWarningOrError() throws ShellException, RemoteException
    {
        final CollectingOutput output = new CollectingOutput();
        final String message = "Test method called";
        final String prompt = "our test prompt";
        AbstractClient client = new AbstractClient( null, null )
        {
            @Override
            public ShellServer getServer()
            {
                ShellServer server = null;
                try
                {
                    server = new GraphDatabaseShellServer( null )
                    {

                        @Override
                        public Response interpretLine( Serializable clientId, String line, Output out )
                                throws ShellException
                        {
                            try
                            {
                                out.println( message );
                            }
                            catch ( RemoteException e )
                            {
                            }
                            return new Response( prompt, line.endsWith( ";" ) ? Continuation.EXCEPTION_CAUGHT
                                                                              : Continuation.INPUT_INCOMPLETE );
                        }
                    };
                }
                catch ( RemoteException e )
                {
                }
                return server;
            }

            @Override
            public Output getOutput()
            {
                return output;
            }
        };

        client.evaluate( "RETURN " );
        assertThat( client.getPrompt(), equalTo( "> " ) );
        client.evaluate( "i;" );

        Set<String> messages = new HashSet<>();
        for ( String s : output )
        {
            messages.add( s );
        }
        assertThat( messages, contains( message ) );
        assertThat( client.getPrompt(), equalTo( prompt ) );
    }
}