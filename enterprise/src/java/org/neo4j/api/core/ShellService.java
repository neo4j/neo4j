package org.neo4j.api.core;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Map;

/**
 * Can start and stop a shell server via reflection if the shell should happen
 * to be on the classpath, else it will gracefully say that it isn't there.
 */
class ShellService
{
    private final NeoService neo;
    private final Object shellServer;
    
    ShellService( NeoService neo, Map<String, Serializable> config )
        throws ShellNotAvailableException, RemoteException
    {
        this.neo = neo;
        if ( !shellDependencyAvailable() )
        {
            throw new ShellNotAvailableException();
        }
        this.shellServer = startShellServer( config );
    }
    
    private boolean shellDependencyAvailable()
    {
        try
        {
            Class.forName( "org.neo4j.shell.ShellServer" );
            return true;
        }
        catch ( Throwable t )
        {
            return false;
        }
    }
    
    private Object startShellServer( Map<String, Serializable> config )
        throws RemoteException
    {
        Integer port = ( Integer )
            getConfig( config, "port", "DEFAULT_PORT" );
        String name = ( String )
            getConfig( config, "name", "DEFAULT_NAME" );
        try
        {
            Class<?> shellServerClass =
                Class.forName( "org.neo4j.shell.neo.NeoShellServer" );
            Object shellServer = shellServerClass.getConstructor(
                NeoService.class ).newInstance( neo );
            shellServer.getClass().getMethod( "makeRemotelyAvailable",
                Integer.TYPE, String.class ).invoke( shellServer, port, name );
            return shellServer;
        }
        catch ( Exception e )
        {
            throw new RemoteException( "Couldn't start shell '" + name +
                "' at port " + port, e );
        }
    }
    
    private Serializable getConfig( Map<String, Serializable> config,
        String key, String defaultVariableName ) throws RemoteException
    {
        Serializable result = config.get( key );
        if ( result == null )
        {
            try
            {
                result = ( Serializable ) Class.forName(
                    "org.neo4j.shell.AbstractServer" ).
                        getDeclaredField( defaultVariableName ).get( null );
            }
            catch ( Exception e )
            {
                throw new RemoteException( "Default variable not found", e );
            }
        }
        return result;
    }

    public boolean shutdown() throws ShellNotAvailableException
    {
        try
        {
            shellServer.getClass().getMethod( "shutdown" ).invoke( shellServer );
            return true;
        }
        catch ( Exception e )
        {
            // TODO Really swallow this? Why not, who cares?
            return false;
        }
    }
    
    static class ShellNotAvailableException extends Exception
    {
        public ShellNotAvailableException()
        {
            super();
        }
    }
}
