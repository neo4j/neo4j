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
package org.neo4j.server.rest.management.console;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.HashMap;

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.server.webadmin.console.ScriptSession;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.impl.AbstractClient;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.impl.ShellServerKernelExtension;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

public class ShellSession implements ScriptSession
{
    private static volatile ShellServer fallbackServer = null;

    private final ShellClient client;
    private final CollectingOutput output;
    private final Log log;

    public ShellSession( GraphDatabaseAPI graph )
    {
        try
        {
            this.log = graph.getDependencyResolver().resolveDependency( LogService.class ).getInternalLog( getClass() );
            ShellServerKernelExtension extension = graph.getDependencyResolver().resolveDependency( KernelExtensions
                    .class ).resolveDependency( ShellServerKernelExtension.class );

            ShellServer server = extension.getServer();
            if ( server == null )
            {
                server = getFallbackServer( graph );
            }
            output = new CollectingOutput();
            client = new SameJvmClient( new HashMap<String, Serializable>(), server, output );
            output.asString();
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( "Unable to start shell client", e );
        }
        catch ( ShellException e )
        {
            throw new RuntimeException( "Unable to start shell client", e );
        }
    }

    private ShellServer getFallbackServer( GraphDatabaseAPI graph )
    {
        if ( fallbackServer == null )
        {
            try
            {
                fallbackServer = new GraphDatabaseShellServer( graph, false );
            }
            catch ( RemoteException e )
            {
                throw new RuntimeException( "Unable to start the fallback shellserver", e );
            }

        }
        return fallbackServer;
    }

    @Override
    public Pair<String, String> evaluate( String script )
    {
        if ( script.equals( "init()" ) )
        {
            return Pair.of( "", client.getPrompt() );
        }
        if ( script.equals( "exit" ) || script.equals( "quit" ) )
        {
            return Pair.of( "Sorry, can't do that.", client.getPrompt() );
        }
        try
        {
            log.debug( script );
            client.evaluate( removeInitialNewline( script ) );
            return Pair.of( output.asString(), client.getPrompt() );
        }
        catch ( ShellException e )
        {
            String message = ((AbstractClient) client).shouldPrintStackTraces() ?
                    ShellException.stackTraceAsString( e ) : ShellException.getFirstMessage( e );
            return Pair.of( message, client.getPrompt() );
        }
    }

    private String removeInitialNewline( String script )
    {
        return script != null && script.startsWith( "\n" ) ? script.substring( 1 ) : script;
    }
}
