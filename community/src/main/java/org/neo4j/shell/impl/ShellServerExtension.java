package org.neo4j.shell.impl;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

@Service.Implementation( KernelExtension.class )
public final class ShellServerExtension extends KernelExtension
{
    public ShellServerExtension()
    {
        super( "shell" );
    }

    @Override
    protected void load( KernelData kernel )
    {
        String shellConfig = (String) kernel.getParam( "enable_remote_shell" );
        if ( shellConfig != null )
        {
            if ( shellConfig.contains( "=" ) )
            {
                enableRemoteShell( kernel, parseShellConfigParameter( shellConfig ) );
            }
            else if ( Boolean.parseBoolean( shellConfig ) )
            {
                enableRemoteShell( kernel, null );
            }
        }
    }

    @Override
    protected void unload( KernelData kernel )
    {
        getServer( kernel ).shutdown();
    }

    @SuppressWarnings( "boxing" )
    public void enableRemoteShell( KernelData kernel, Map<String, Serializable> config )
    {
        GraphDatabaseShellServer server = getServer( kernel );
        if ( server != null )
        {
            throw new IllegalStateException( "Shell already enabled" );
        }
        if ( config == null ) config = Collections.<String, Serializable>emptyMap();
        try
        {
            server = new GraphDatabaseShellServer( kernel.graphDatabase() );
            int port = (Integer) getConfig( config, "port", "DEFAULT_PORT" );
            String name = (String) getConfig( config, "name", "DEFAULT_NAME" );
            server.makeRemotelyAvailable( port, name );
        }
        catch ( Exception ex )
        {
            if ( server != null ) server.shutdown();
            throw new IllegalStateException( "Can't start remote Neo4j shell", ex );
        }
        setServer( kernel, server );
    }

    private void setServer( KernelData kernel, final GraphDatabaseShellServer server )
    {
        kernel.setState( this, server );
    }

    private GraphDatabaseShellServer getServer( KernelData kernel )
    {
        return (GraphDatabaseShellServer) kernel.getState( this );
    }

    @SuppressWarnings( "boxing" )
    private static Map<String, Serializable> parseShellConfigParameter( String shellConfig )
    {
        Map<String, Serializable> map = new HashMap<String, Serializable>();
        for ( String keyValue : shellConfig.split( "," ) )
        {
            String[] splitted = keyValue.split( "=" );
            if ( splitted.length != 2 )
            {
                throw new RuntimeException(
                        "Invalid shell configuration '" + shellConfig
                                + "' should be '<key1>=<value1>,<key2>=<value2>...' where key can"
                                + " be any of [port, name]" );
            }
            String key = splitted[0].trim();
            Serializable value = splitted[1];
            if ( key.equals( "port" ) )
            {
                value = Integer.parseInt( splitted[1] );
            }
            map.put( key, value );
        }
        return map;
    }

    private Serializable getConfig( Map<String, Serializable> config, String key,
            String defaultVariableName ) throws RemoteException
    {
        Serializable result = config.get( key );
        if ( result == null )
        {
            try
            {
                result = (Serializable) AbstractServer.class.getDeclaredField(
                        defaultVariableName ).get( null );
            }
            catch ( Exception e )
            {
                throw new RemoteException( "Default variable not found", e );
            }
        }
        return result;
    }
}
