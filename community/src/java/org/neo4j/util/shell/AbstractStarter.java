package org.neo4j.util.shell;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AbstractStarter
{
    protected static void setSessionVariablesFromArgs(
        ShellClient client, String[] args ) throws RemoteException
    {
        Map<String, String> map = parseArgs( args );
        String profile = map.get( "profile" );
        if ( profile != null )
        {
            applyProfileFile( new File( profile ), client );
        }
        
        for ( Map.Entry<String, String> entry : map.entrySet() )
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
    
    private static boolean isOption( String arg )
    {
        return arg.startsWith( "-" );
    }
    
    private static String stripOption( String arg )
    {
        return arg.substring( 1 );
    }

    protected static Map<String, String> parseArgs( String[] args )
    {
        Map<String, String> map = new HashMap<String, String>();
        for ( int i = 0; i < args.length; i++ )
        {
            String arg = args[ i ];
            if ( isOption( arg ) )
            {
                arg = stripOption( arg );
                int equalIndex = arg.indexOf( '=' );
                if ( equalIndex != -1 )
                {
                    String key = arg.substring( 0, equalIndex );
                    String value = arg.substring( equalIndex + 1 );
                    map.put( key, value );
                }
                else
                {
                    String key = arg;
                    int nextIndex = i + 1;
                    String value = nextIndex < args.length ?
                        args[ nextIndex ] : null;
                    value = isOption( value ) ? null : value;
                    map.put( key, value );
                }
            }
        }
        return map;
    }

//    protected static String getArg( String[] args, String name )
//    {
//        for ( String arg : args )
//        {
//            if ( arg.startsWith( "-" ) )
//            {
//                arg = arg.substring( 1 );
//                String[] keyAndValue = splitArgIntoKeyAndValue( arg );
//                if ( keyAndValue[ 0 ].equals( name ) )
//                {
//                    return keyAndValue[ 1 ];
//                }
//            }
//        }
//        return null;
//    }
}
