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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Args;
import org.neo4j.kernel.Version;
import org.neo4j.shell.impl.RmiLocation;
import org.neo4j.shell.impl.ShellBootstrap;
import org.neo4j.shell.impl.SimpleAppServer;
import org.neo4j.shell.impl.SystemOutput;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

import static org.neo4j.io.fs.FileUtils.newBufferedFileReader;
import static org.neo4j.kernel.impl.util.Charsets.UTF_8;

/**
 * Can start clients, either remotely to another JVM running a server
 * or by starting a local {@link GraphDatabaseShellServer} in this JVM
 * and connecting to it.
 */
public class StartClient
{
    private AtomicBoolean hasBeenShutdown = new AtomicBoolean();

    /**
     * Prints the version and edition of neo4j and exits.
     */
    public static final String ARG_VERSION = "version";

    /**
     * The path to the local (this JVM) {@link GraphDatabaseService} to
     * start and connect to.
     */
    public static final String ARG_PATH = "path";

    /**
     * Whether or not the shell client should be readonly.
     */
    public static final String ARG_READONLY = "readonly";

    /**
     * The host (ip or name) to connect remotely to.
     */
    public static final String ARG_HOST = "host";

    /**
     * The port to connect remotely on. A server must have been started
     * on that port.
     */
    public static final String ARG_PORT = "port";

    /**
     * The RMI name to use.
     */
    public static final String ARG_NAME = "name";

    /**
     * The PID (process ID) to connect to.
     */
    public static final String ARG_PID = "pid";

    /**
     * Commands (a line can contain more than one command, with &amp;&amp; in between)
     * to execute when the shell client has been connected.
     */
    public static final String ARG_COMMAND = "c";

    /**
     * File a file with shell commands to execute when the shell client has
     * been connected.
     */
    public static final String ARG_FILE = "file";

    /**
     * Special character used to request reading from stdin rather than a file.
     * Uses the dash character, which is a common way to specify this.
     */
    public static final String ARG_FILE_STDIN = "-";

    /**
     * Configuration file to load and use if a local {@link GraphDatabaseService}
     * is started in this JVM.
     */
    public static final String ARG_CONFIG = "config";

    private final GraphDatabaseFactory factory;
    private final PrintStream out;
    private final PrintStream err;

    // Visible for testing
    StartClient( PrintStream out, PrintStream err )
    {
        this.factory = loadEditionDatabaseFactory();
        this.out = out;
        this.err = err;
    }

    /**
     * Starts a shell client. Remote or local depending on the arguments.
     *
     * @param arguments the arguments from the command line. Can contain
     * information about whether to start a local
     * {@link GraphDatabaseShellServer} or connect to an already running
     * {@link GraphDatabaseService}.
     */
    public static void main( String[] arguments )
    {
        InterruptSignalHandler signalHandler = InterruptSignalHandler.getHandler();
        try
        {
            new StartClient( System.out, System.err ).start( arguments, signalHandler );
        }
        catch ( ShellExecutionFailureException e )
        {
            e.dumpMessage( System.out, System.err );
            System.exit( 1 );
        }
    }

    private static GraphDatabaseFactory loadEditionDatabaseFactory()
    {
        GraphDatabaseFactory factory;
        try
        {
            factory = (GraphDatabaseFactory) Class.forName( "org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory" )
                    .newInstance();
        }
        catch ( Exception e )
        {
            factory = new GraphDatabaseFactory();
        }
        return factory;
    }

    // visible for testing
    void start( String[] arguments, CtrlCHandler signalHandler )
    {
        Args args = Args.withFlags( ARG_READONLY ).parse( arguments );
        if ( args.has( "?" ) || args.has( "h" ) || args.has( "help" ) || args.has( "usage" ) )
        {
            printUsage( out );
            return;
        }

        String path = args.get( ARG_PATH, null );
        String host = args.get( ARG_HOST, null );
        String port = args.get( ARG_PORT, null );
        String name = args.get( ARG_NAME, null );
        String pid = args.get( ARG_PID, null );
        boolean version = args.getBoolean( ARG_VERSION, false, true );

        if ( version )
        {
            out.printf( "Neo4j %s, version %s", factory.getEdition(), Version.getKernelVersion() );
        }
        else if ( (path != null && (port != null || name != null || host != null || pid != null))
             || (pid != null && host != null) )
        {
            err.println( "You have supplied both " +
                         ARG_PATH + " as well as " + ARG_HOST + "/" + ARG_PORT + "/" + ARG_NAME + ". " +
                         "You should either supply only " + ARG_PATH +
                         " or " + ARG_HOST + "/" + ARG_PORT + "/" + ARG_NAME + " so that either a local or " +
                         "remote shell client can be started" );
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
                throw new ShellExecutionFailureException( e, args );
            }
            startLocal( args, signalHandler );
        }
        // Remote
        else
        {
            String readonly = args.get( ARG_READONLY, null );
            if ( readonly != null )
            {
                err.println( "Warning: -" + ARG_READONLY + " is ignored unless you connect with -" + ARG_PATH + "!" );
            }

            // Start server on the supplied process
            if ( pid != null )
            {
                startServer( pid, args );
            }
            startRemote( args, signalHandler );
        }
    }

    private static final Method attachMethod, loadMethod;

    static
    {
        Method attach, load;
        try
        {
            Class<?> vmClass = Class.forName( "com.sun.tools.attach.VirtualMachine" );
            attach = vmClass.getMethod( "attach", String.class );
            load = vmClass.getMethod( "loadAgent", String.class, String.class );
        }
        catch ( Exception e )
        {
            attach = load = null;
        }
        attachMethod = attach;
        loadMethod = load;
    }

    private static void checkNeo4jDependency() throws ShellException
    {
        try
        {
            Class.forName( "org.neo4j.graphdb.GraphDatabaseService" );
        }
        catch ( Exception e )
        {
            throw new ShellException( "Neo4j not found on the classpath" );
        }
    }

    private void startLocal( Args args, CtrlCHandler signalHandler )
    {
        String dbPath = args.get( ARG_PATH, null );
        if ( dbPath == null )
        {
            err.println( "ERROR: To start a local Neo4j service and a " +
                         "shell client on top of that you need to supply a path to a " +
                         "Neo4j store or just a new path where a new store will " +
                         "be created if it doesn't exist. -" + ARG_PATH +
                         " /my/path/here" );
            return;
        }

        try
        {
            boolean readOnly = args.getBoolean( ARG_READONLY, false, true );
            tryStartLocalServerAndClient( dbPath, readOnly, args, signalHandler );
        }
        catch ( Exception e )
        {
            throw new ShellExecutionFailureException( e, args );
        }
    }

    private void tryStartLocalServerAndClient( String dbPath, boolean readOnly, Args args,
            CtrlCHandler signalHandler ) throws Exception
    {
        String configFile = args.get( ARG_CONFIG, null );
        final GraphDatabaseShellServer server = getGraphDatabaseShellServer( dbPath, readOnly, configFile );
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                shutdownIfNecessary( server );
            }
        } );

        if ( !isCommandLine( args ) )
        {
            out.println( "NOTE: Local Neo4j graph database service at '" + dbPath + "'" );
        }
        ShellClient client = ShellLobby.newClient( server, getSessionVariablesFromArgs( args ),
                new SystemOutput( out ), signalHandler );
        grabPromptOrJustExecuteCommand( client, args );

        shutdownIfNecessary( server );
    }

    protected GraphDatabaseShellServer getGraphDatabaseShellServer( String dbPath, boolean readOnly, String configFile )
            throws RemoteException
    {
        return new GraphDatabaseShellServer( factory, dbPath, readOnly, configFile );
    }

    private void shutdownIfNecessary( ShellServer server )
    {
        try
        {
            if ( hasBeenShutdown.compareAndSet( false, true ) )
            {
                server.shutdown();
            }
        }
        catch ( RemoteException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void startServer( String pid, Args args )
    {
        String port = args.get( "port", Integer.toString( SimpleAppServer.DEFAULT_PORT ) );
        String name = args.get( "name", SimpleAppServer.DEFAULT_NAME );
        try
        {
            String jarfile = new File(
                    getClass().getProtectionDomain().getCodeSource().getLocation().toURI() ).getAbsolutePath();
            Object vm = attachMethod.invoke( null, pid );
            loadMethod.invoke( vm, jarfile, new ShellBootstrap( Integer.parseInt( port ), name ).serialize() );
        }
        catch ( Exception e )
        {
            throw new ShellExecutionFailureException( e, args );
        }
    }

    private void startRemote( Args args, CtrlCHandler signalHandler )
    {
        try
        {
            String host = args.get( ARG_HOST, "localhost" );
            int port = args.getNumber( ARG_PORT, SimpleAppServer.DEFAULT_PORT ).intValue();
            String name = args.get( ARG_NAME, SimpleAppServer.DEFAULT_NAME );
            ShellClient client = ShellLobby.newClient( RmiLocation.location( host, port, name ),
                    getSessionVariablesFromArgs( args ), signalHandler );
            if ( !isCommandLine( args ) )
            {
                out.println( "NOTE: Remote Neo4j graph database service '" + name + "' at port " + port );
            }
            grabPromptOrJustExecuteCommand( client, args );
        }
        catch ( Exception e )
        {
            throw new ShellExecutionFailureException( e, args );
        }
    }

    private static boolean isCommandLine( Args args )
    {
        return args.get( ARG_COMMAND, null ) != null ||
               args.get( ARG_FILE, null ) != null;
    }

    private void grabPromptOrJustExecuteCommand( ShellClient client, Args args ) throws Exception
    {
        String command = args.get( ARG_COMMAND, null );
        if ( command != null )
        {
            client.evaluate( command );
            client.shutdown();
            return;
        }
        String fileName = args.get( ARG_FILE, null );
        if ( fileName != null )
        {
            BufferedReader reader = null;
            try
            {
                if ( fileName.equals( ARG_FILE_STDIN ) )
                {
                    reader = new BufferedReader( new InputStreamReader( System.in, UTF_8 ) );
                }
                else
                {
                    File file = new File( fileName );
                    if ( !file.exists() )
                    {
                        throw new ShellException( "File to execute " + "does not exist: " + fileName );
                    }
                    reader = newBufferedFileReader( file, UTF_8 );
                }
                executeCommandStream( client, reader );
            }
            finally
            {
                if ( reader != null )
                {
                    reader.close();
                }
            }
            return;
        }
        client.grabPrompt();
    }

    private void executeCommandStream( ShellClient client, BufferedReader reader ) throws IOException,
            ShellException
    {
        try
        {
            for ( String line; (line = reader.readLine()) != null; )
            {
                client.evaluate( line );
            }
        }
        finally
        {
            client.shutdown();
            reader.close();
        }
    }

    static Map<String,Serializable> getSessionVariablesFromArgs( Args args ) throws RemoteException, ShellException
    {
        String profile = args.get( "profile", null );
        Map<String,Serializable> session = new HashMap<>();
        if ( profile != null )
        {
            applyProfileFile( new File( profile ), session );
        }

        for ( Map.Entry<String,String> entry : args.asMap().entrySet() )
        {
            String key = entry.getKey();
            if ( key.startsWith( "D" ) )
            {
                key = key.substring( 1 );
                session.put( key, entry.getValue() );
            }
        }
        if ( isCommandLine( args ) )
        {
            session.put( "quiet", true );
        }
        return session;
    }

    private static void applyProfileFile( File file, Map<String,Serializable> session ) throws ShellException
    {
        try ( FileInputStream fis = new FileInputStream( file ) )
        {
            Properties properties = new Properties();
            properties.load( fis );
            for ( Object key : properties.keySet() )
            {
                String stringKey = (String) key;
                String value = properties.getProperty( stringKey );
                session.put( stringKey, value );
            }
        }
        catch ( IOException e )
        {
            throw new IllegalArgumentException( "Couldn't find profile '" +
                                                file.getAbsolutePath() + "'" );
        }
    }

    private static int longestString( String... strings )
    {
        int length = 0;
        for ( String string : strings )
        {
            if ( string.length() > length )
            {
                length = string.length();
            }
        }
        return length;
    }

    private static void printUsage( PrintStream out )
    {
        int port = SimpleAppServer.DEFAULT_PORT;
        String name = SimpleAppServer.DEFAULT_NAME;
        int longestArgLength = longestString( ARG_FILE, ARG_COMMAND,
                ARG_CONFIG,
                ARG_HOST, ARG_NAME,
                ARG_PATH, ARG_PID, ARG_PORT, ARG_READONLY );
        out.println(
                padArg( ARG_HOST, longestArgLength ) + "Domain name or IP of host to connect to (default: localhost)" +
                "\n" +
                padArg( ARG_PORT, longestArgLength ) + "Port of host to connect to (default: " +
                SimpleAppServer.DEFAULT_PORT + ")\n" +
                padArg( ARG_NAME, longestArgLength ) + "RMI name, i.e. rmi://<host>:<port>/<name> (default: "
                + SimpleAppServer.DEFAULT_NAME + ")\n" +
                padArg( ARG_PID, longestArgLength ) + "Process ID to connect to\n" +
                padArg( ARG_COMMAND, longestArgLength ) + "Command line to execute. After executing it the " +
                "shell exits\n" +
                padArg( ARG_FILE, longestArgLength ) + "File containing commands to execute, or '-' to read " +
                "from stdin. After executing it the shell exits\n" +
                padArg( ARG_READONLY, longestArgLength ) + "Connect in readonly mode (only for connecting " +
                "with -" + ARG_PATH + ")\n" +
                padArg( ARG_PATH, longestArgLength ) + "Points to a neo4j db path so that a local server can " +
                "be started there\n" +
                padArg( ARG_CONFIG, longestArgLength ) + "Points to a config file when starting a local " +
                "server\n\n" +

                "Example arguments for remote:\n" +
                "\t-" + ARG_PORT + " " + port + "\n" +
                "\t-" + ARG_HOST + " " + "192.168.1.234" + " -" + ARG_PORT + " " + port + " -" + ARG_NAME + "" +
                " " + name + "\n" +
                "\t-" + ARG_HOST + " " + "localhost" + " -" + ARG_READONLY + "\n" +
                "\t...or no arguments for default values\n" +
                "Example arguments for local:\n" +
                "\t-" + ARG_PATH + " /path/to/db" + "\n" +
                "\t-" + ARG_PATH + " /path/to/db -" + ARG_CONFIG + " /path/to/neo4j.config" + "\n" +
                "\t-" + ARG_PATH + " /path/to/db -" + ARG_READONLY
        );
    }

    private static String padArg( String arg, int length )
    {
        return " -" + pad( arg, length ) + "  ";
    }

    private static String pad( String string, int length )
    {
        // Rather inefficient
        while ( string.length() < length )
        {
            string = string + " ";
        }
        return string;
    }

    private static class ShellExecutionFailureException extends RuntimeException
    {
        private final Throwable cause;
        private final Args args;

        ShellExecutionFailureException( Throwable cause, Args args )
        {
            this.cause = cause;
            this.args = args;
        }

        private void dumpMessage( PrintStream out, PrintStream err )
        {
            String message = cause.getCause() instanceof ConnectException ?
                             "Connection refused" : cause.getMessage();
            err.println( "ERROR (-v for expanded information):\n\t" + message );
            if ( args.has( "v" ) )
            {
                cause.printStackTrace( err );
            }
            err.println();
            printUsage( out );
        }
    }
}
