/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.shell;

import java.util.Map;

import org.neo4j.shell.impl.AbstractServer;
import org.neo4j.shell.impl.AbstractStarter;

/**
 * Convenience main class for starting a client which connects to a remote
 * server. Which port/name to connect to may be specified as arguments.
 */
public class StartRemoteClient extends AbstractStarter
{
    public static final String ARG_PORT = "port";
    public static final String ARG_NAME = "name";
    
    /**
     * Starts a client and connects to a remote server.
     * 
     * @param args may contain RMI port/name to the server.
     */
    public static void main( String[] args )
    {
        Map<String, String> argMap = parseArgs( args );
        try
        {
            int port = getPort( argMap );
            String name = getShellName( argMap );
            ShellClient client = ShellLobby.newClient( port, name );
            System.out.println( "NOTE: Connected to remote neo4j service '" + name +
                    "' at port " + port );
            setSessionVariablesFromArgs( client, args );
            client.grabPrompt();
        }
        catch ( Exception e )
        {
            handleException( e, argMap );
        }
    }

    private static int getPort( Map<String, String> argMap )
    {
        String arg = argMap.get( ARG_PORT );
        return arg != null ? Integer.parseInt( arg ) :
            AbstractServer.DEFAULT_PORT;
    }

    private static String getShellName( Map<String, String> argMap )
    {
        String arg = argMap.get( ARG_NAME );
        return arg != null ? arg : AbstractServer.DEFAULT_NAME;
    }
}
