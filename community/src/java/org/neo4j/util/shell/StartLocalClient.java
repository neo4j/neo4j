/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
package org.neo4j.util.shell;

public class StartLocalClient
{
    public static void main( String[] args )
    {
        if ( args.length == 0 )
        {
            System.out.println( "ERROR: To start a local neo service and a " +
                "shell client on top of that you need to supply a path to a " +
                "neo store or just a new path where a new neo store will " +
                "be created if it doesn't exist" );
            return;
        }
        
        String neoDbPath = args[ 0 ];
        try
        {
            tryStartLocalServerAndClient( neoDbPath );
        }
        catch ( Exception e )
        {
            System.err.println( "Can't start client with local neo service: " +
                e );
            e.printStackTrace( System.err );
        }
    }

    private static void tryStartLocalServerAndClient( String neoDbPath )
        throws Exception
    {
        final LocalNeoShellServer server =
            new LocalNeoShellServer( neoDbPath );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                server.shutdown();
            }
        } );
        
        System.out.println( "NOTE: Using local neo service at '" +
            neoDbPath + "'" );
        new SameJvmClient( server ).grabPrompt();
        server.shutdown();
    }
}
