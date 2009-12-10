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
package org.neo4j.shell;

import java.util.Map;

import org.neo4j.shell.neo.LocalNeoShellServer;

public class StartLocalClient extends AbstractStarter
{
    public static final String ARG_PATH = "path";
    public static final String ARG_READONLY = "readonly";
    
    public static void main( String[] args )
    {
        Map<String, String> argMap = parseArgs( args );
        String neoDbPath = argMap.get( ARG_PATH );
        if ( neoDbPath == null )
        {
            System.err.println( "ERROR: To start a local neo service and a " +
                "shell client on top of that you need to supply a path to a " +
                "neo store or just a new path where a new neo store will " +
                "be created if it doesn't exist. -" + ARG_PATH +
                " /my/path/here" );
            return;
        }
        
        try
        {
            boolean readOnly = argMap.containsKey( ARG_READONLY ) &&
                stringAsBoolean( argMap.get( ARG_READONLY ), true );
            tryStartLocalServerAndClient( neoDbPath, readOnly, args );
        }
        catch ( Exception e )
        {
            if ( storeWasLocked( e ) )
            {
                if ( wantToConnectReadOnlyInstead() )
                {
                    try
                    {
                        tryStartLocalServerAndClient( neoDbPath, true, args );
                    }
                    catch ( Exception innerException )
                    {
                        handleException( innerException );
                    }
                }
                else
                {
                    handleException( e );
                }
            }
            else
            {
                handleException( e );
            }
        }
        System.exit( 0 );
    }

    private static boolean wantToConnectReadOnlyInstead()
    {
        Console console = new StandardConsole();
        console.format( "\nThe store seem locked. Start a read-only client " +
        	"instead (y/n) [y]? " );
        String input = console.readLine();
        return input.length() == 0 || input.equals( "y" );
    }

    private static boolean storeWasLocked( Exception e )
    {
        // TODO Fix this when a specific exception is thrown
        return mineException( e, IllegalStateException.class,
            "Unable to lock store" );
    }

    private static boolean mineException( Throwable e,
        Class<IllegalStateException> eClass, String startOfMessage )
    {
        if ( eClass.isInstance( e ) &&
            e.getMessage().startsWith( startOfMessage ) )
        {
            return true;
        }
        
        Throwable cause = e.getCause();
        if ( cause != null )
        {
            return mineException( cause, eClass, startOfMessage );
        }
        return false;
    }

    private static void handleException( Exception e )
    {
        System.err.println( "Can't start client with local neo service: " +
            e );
        e.printStackTrace( System.err );
        System.exit( 1 );
    }

    private static void tryStartLocalServerAndClient( String neoDbPath,
        boolean readOnly, String[] args ) throws Exception
    {
        final LocalNeoShellServer server =
            new LocalNeoShellServer( neoDbPath, readOnly );
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
        ShellClient client = new SameJvmClient( server );
        setSessionVariablesFromArgs( client, args );
        client.grabPrompt();
        server.shutdown();
    }
}
