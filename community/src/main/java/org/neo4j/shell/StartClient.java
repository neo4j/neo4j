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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.commons.commandline.Args;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.EmbeddedReadOnlyGraphDatabase;
import org.neo4j.shell.impl.AbstractServer;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.impl.StandardConsole;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

/**
 * Can start clients, either via {@link StartRemoteClient} or
 * {@link StartLocalClient}.
 */
public class StartClient
{
    private AtomicBoolean hasBeenShutdown = new AtomicBoolean();
    public static final String ARG_PATH = "path";
    public static final String ARG_READONLY = "readonly";
    public static final String ARG_PORT = "port";
    public static final String ARG_NAME = "name";
    
    private StartClient()
    {
    }
    
    /**
     * Starts a shell client. Remote or local depending on the arguments.
     * @param args the arguments from the command line. Can contain
     * information about whether to start a local
     * {@link GraphDatabaseShellServer} or connect to an already running
     * {@link GraphDatabaseService}.
     */
    public static void main( String[] arguments )
    {
        new StartClient().start( arguments );
    }
    
    private void start( String[] arguments )
    {
        Args args = new Args( arguments );
        if ( args.has( "?" ) || args.has( "h" ) || args.has( "help" ) || args.has( "usage" ) )
        {
            printUsage();
            return;
        }
        
        String path = args.get( ARG_PATH, null );
        String port = args.get( ARG_PORT, null );
        String name = args.get( ARG_NAME, null );
        
        if ( path != null && ( port != null || name != null ) )
        {
            System.err.println( "You have supplied both " +
                ARG_PATH + " as well as " + ARG_PORT + "/" + ARG_NAME + ". " +
                "You should either supply only " + ARG_PATH +
                " or " + ARG_PORT + "/" + ARG_NAME + " so that either a local or " +
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
                handleException( e, args );
            }
            startLocal( args );
        }
        // Remote
        else
        {
            startRemote( args );
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

    private void startLocal( Args args )
    {
        String dbPath = args.get( ARG_PATH, null );
        if ( dbPath == null )
        {
            System.err.println( "ERROR: To start a local Neo4j service and a " +
                "shell client on top of that you need to supply a path to a " +
                "Neo4j store or just a new path where a new store will " +
                "be created if it doesn't exist. -" + ARG_PATH +
                " /my/path/here" );
            return;
        }
        
        try
        {
            boolean readOnly = args.getBoolean( ARG_READONLY, false );
            tryStartLocalServerAndClient( dbPath, readOnly, args );
        }
        catch ( Exception e )
        {
            if ( storeWasLocked( e ) )
            {
                if ( wantToConnectReadOnlyInstead() )
                {
                    try
                    {
                        tryStartLocalServerAndClient( dbPath, true, args );
                    }
                    catch ( Exception innerException )
                    {
                        handleException( innerException, args );
                    }
                }
                else
                {
                    handleException( e, args );
                }
            }
            else
            {
                handleException( e, args );
            }
        }
        System.exit( 0 );
    }

    private static boolean wantToConnectReadOnlyInstead()
    {
        Console console = new StandardConsole();
        console.format( "\nThe store seem locked. Start a read-only client " +
            "instead (y/n) [y]? " );
        String input = console.readLine( "" );
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

    private void tryStartLocalServerAndClient( String dbPath,
        boolean readOnly, Args args ) throws Exception
    {
        final GraphDatabaseService graph = readOnly ?
            new EmbeddedReadOnlyGraphDatabase( dbPath ) :
            new EmbeddedGraphDatabase( dbPath );
        
        final GraphDatabaseShellServer server =
            new GraphDatabaseShellServer( graph );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                shutdownIfNecessary( server, graph );
            }
        } );
        
        System.out.println( "NOTE: Local Neo4j graph database service at '" +
            dbPath + "'" );
        ShellClient client = new SameJvmClient( server );
        setSessionVariablesFromArgs( client, args );
        client.grabPrompt();
        shutdownIfNecessary( server, graph );
    }

    private void shutdownIfNecessary( ShellServer server,
            GraphDatabaseService graphDb )
    {
        try
        {
            if ( !hasBeenShutdown.compareAndSet( false, true ) )
            {
                server.shutdown();
                graphDb.shutdown();
            }
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static void startRemote( Args args )
    {
        try
        {
            int port = args.getNumber( ARG_PORT, AbstractServer.DEFAULT_PORT ).intValue();
            String name = args.get( ARG_NAME, AbstractServer.DEFAULT_NAME );
            ShellClient client = ShellLobby.newClient( port, name );
            System.out.println( "NOTE: Remote Neo4j graph database service '" + name +
                    "' at port " + port );
            setSessionVariablesFromArgs( client, args );
            client.grabPrompt();
        }
        catch ( Exception e )
        {
            handleException( e, args );
        }
    }

    protected static void setSessionVariablesFromArgs(
        ShellClient client, Args args ) throws RemoteException
    {
        String profile = args.get( "profile", null );
        if ( profile != null )
        {
            applyProfileFile( new File( profile ), client );
        }
        
        for ( Map.Entry<String, String> entry : args.asMap().entrySet() )
        {
            String key = entry.getKey();
            if ( key.startsWith( "D" ) )
            {
                key = key.substring( 1 );
                client.session().set( key, entry.getValue() );
            }
        }
    }
    
    private static void applyProfileFile( File file, ShellClient client )
    {
        InputStream in = null;
        try
        {
            Properties properties = new Properties();
            properties.load( new FileInputStream( file ) );
            for ( Object key : properties.keySet() )
            {
                String stringKey = ( String ) key;
                String value = properties.getProperty( stringKey );
                client.session().set( stringKey, value );
            }
        }
        catch ( IOException e )
        {
            throw new IllegalArgumentException( "Couldn't find profile '" +
                file.getAbsolutePath() + "'" );
        }
        finally
        {
            if ( in != null )
            {
                try
                {
                    in.close();
                }
                catch ( IOException e )
                {
                    // OK
                }
            }
        }
    }
    
    private static void handleException( Exception e, Args args )
    {
        String message = e.getCause() instanceof ConnectException ?
                "Connection refused" : e.getMessage();
        System.err.println( "ERROR (-v for expanded information):\n\t" + message );
        if ( args.has( "v" ) )
        {
            e.printStackTrace( System.err );
        }
        System.err.println();
        printUsage();
        System.exit( 1 );
    }

    private static void printUsage()
    {
        int port = AbstractServer.DEFAULT_PORT;
        String name = AbstractServer.DEFAULT_NAME;
        String pathArg = StartClient.ARG_PATH;
        String portArg = StartClient.ARG_PORT;
        String nameArg = StartClient.ARG_NAME;
        System.out.println(
            "Example arguments for remote:\n" +
                "\t-" + portArg + " " + port + "\n" +
                "\t-" + portArg + " " + port +
                    " -" + nameArg + " " + name + "\n" +
                "\t...or no arguments\n" +
            "Example arguments for local:\n" +
                "\t-" + pathArg + " /path/to/db" + "\n" +
                "\t-" + pathArg + " /path/to/db -readonly"
        );
    }
}
