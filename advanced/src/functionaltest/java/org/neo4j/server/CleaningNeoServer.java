package org.neo4j.server;

import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.web.Jetty6WebServer;

import java.io.File;
import java.util.Arrays;

public class CleaningNeoServer extends NeoServer
{
    private final String dir;
    private static String lastOpened;

    public CleaningNeoServer( final AddressResolver addressResolver, final StartupHealthCheck startupHealthCheck,
                              final File configFile, final Jetty6WebServer jetty6WebServer, final String dir )
    {
        super( addressResolver, startupHealthCheck, configFile, jetty6WebServer );
        this.dir = dir;
        if ( lastOpened != null )
        {
            String message = lastOpened + " didn't shut down the server correctly!";
            lastOpened = null;
            throw new RuntimeException( message );
        }
        lastOpened = originatingTestClass();
    }

    private String originatingTestClass()
    {
        try
        {
            throw new RuntimeException();
        } catch ( RuntimeException e )
        {
            for ( StackTraceElement el : Arrays.asList( e.getStackTrace() ) )
            {
                String className = el.getClassName();
                if ( className.contains( "Test" ) )
                {
                    return className;
                }
            }
        }

        return "N/A";
    }

    @Override
    public void stop()
    {
        super.stop();
        recursiveDelete( dir );
        lastOpened = null;
    }

    private void secureDelete( File f )
    {
        boolean success = f.delete();
        if ( !success )
        {
            throw new RuntimeException( "Failed to delete the temporary database" );
        }
    }

    public void recursiveDelete( String dirOrFile )
    {
        recursiveDelete( new File( dirOrFile ) );
    }

    public void recursiveDelete( File dirOrFile )
    {
        if ( dirOrFile.isDirectory() )
        {
            for ( File sub : dirOrFile.listFiles() )
            {
                recursiveDelete( sub );
            }
        }

        secureDelete( dirOrFile );
    }
}
