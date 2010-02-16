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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.shell.impl.AbstractStarter;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

/**
 * Can start clients, either via {@link StartRemoteClient} or
 * {@link StartLocalClient}.
 */
public class StartClient extends AbstractStarter
{
    /**
     * Starts a shell client. Remote or local depending on the arguments.
     * @param args the arguments from the command line. Can contain
     * information about whether to start a local
     * {@link GraphDatabaseShellServer} or connect to an already running
     * {@link GraphDatabaseService}.
     */
    public static void main( String[] args )
    {
        Map<String, String> argMap = parseArgs( args );
        if ( argMap.containsKey( "?" ) || argMap.containsKey( "h" ) ||
                argMap.containsKey( "help" ) || argMap.containsKey( "usage" ) )
        {
            printUsage();
            return;
        }
        
        String path = argMap.get( StartLocalClient.ARG_PATH );
        String port = argMap.get( StartRemoteClient.ARG_PORT );
        String name = argMap.get( StartRemoteClient.ARG_NAME );
        
        if ( path != null && ( port != null || name != null ) )
        {
            System.err.println( "You have supplied both " +
                StartLocalClient.ARG_PATH + " as well as " +
                StartRemoteClient.ARG_PORT + "/" +
                StartRemoteClient.ARG_NAME + ". " +
                "You should either supply only " + StartLocalClient.ARG_PATH +
                " or " + StartRemoteClient.ARG_PORT + "/" +
                StartRemoteClient.ARG_NAME + " so that either a local or " +
                "remote shell client can be started" );
            return;
        }
        // Local
        else if ( path != null )
        {
            try
            {
                checkNeo4jDependency();
            }
            catch ( ShellException e )
            {
                handleException( e, argMap );
            }
            StartLocalClient.main( args );
        }
        // Remote
        else
        {
            StartRemoteClient.main( args );
        }
    }

    private static void checkNeo4jDependency() throws ShellException
    {
        try
        {
            Class.forName( "org.neo4j.graphdb.GraphDatabaseService" );
        }
        catch ( Exception e )
        {
            throw new ShellException( "Neo4j not found on the classpath", e );
        }
    }
}
