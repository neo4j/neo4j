package org.neo4j.util.shell;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.Properties;

public class AbstractStarter
{
    protected static String getNeoPathFromArgs( String[] args )
        throws RemoteException
    {
        for ( String arg : args )
        {
            if ( !arg.startsWith( "-" ) )
            {
                return arg;
            }
        }
        throw new IllegalArgumentException( "No neo path given" );
    }
    
    protected static void setSessionVariablesFromArgs(
        ShellClient client, String[] args ) throws RemoteException
    {
        for ( String arg : args )
        {
            if ( arg.startsWith( "-" ) )
            {
                arg = arg.substring( 1 );
                String[] keyAndValue = splitArgIntoKeyAndValue( arg );
                if ( keyAndValue[ 0 ].equals( "profile" ) )
                {
                    File file = new File( keyAndValue[ 1 ] );
                    applyProfile( file, client );
                }
                else if ( keyAndValue[ 0 ].startsWith( "D" ) )
                {
                    String key = keyAndValue[ 0 ].substring( 1 );
                    client.session().set( key, keyAndValue[ 1 ] );
                }
                else
                {
                    throw new IllegalArgumentException( "Invalid argument '" +
                        arg + "', expected profile=<file> or Dkey=value" );
                }
            }
        }
    }

    private static void applyProfile( File file, ShellClient client )
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

    private static String[] splitArgIntoKeyAndValue( String arg )
    {
        int index = arg.indexOf( '=' );
        if ( index == -1 )
        {
            throw new IllegalArgumentException( "Invalid argument '" + arg +
                "' needs to be in the form key=value" );
        }
        String key = arg.substring( 0, index );
        String value = arg.substring( index + 1 );
        return new String[] { key, value };
    }
}
