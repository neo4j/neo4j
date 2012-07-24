package org.neo4j.perftest.enterprise.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Parameters
{
    public static Conversion<String[], Configuration> configuration( final Configuration defaultConfiguration,
                                                                     Setting<?>... settings )
    {
        final Parameters parser = parser( settings );
        return new Conversion<String[], Configuration>()
        {
            @Override
            public Configuration convert( String[] args )
            {
                Configuration.Builder configurationBuilder = Configuration.builder();
                if ( parser.parse( configurationBuilder, args ).length != 0 )
                {
                    throw new IllegalArgumentException( "Positional arguments not supported." );
                }
                return Configuration.combine( configurationBuilder.build(), defaultConfiguration );
            }
        };
    }

    public String[] parse( Configuration.Builder configurationBuilder, String... args )
    {
        for ( int i = 0; i < args.length; i++ )
        {
            if ( args[i].charAt( 0 ) == '-' )
            {
                String flag = args[i].substring( 1 );
                Setting<?> setting = settings.get( flag );
                if ( setting == null )
                {
                    throw new IllegalArgumentException( String.format( "Unknown parameter '%s'", args[i] ) );
                }
                if ( setting.isBoolean() )
                {
                    configurationBuilder.set( setting, "true" );
                    continue;
                }
                if ( ++i == args.length )
                {
                    throw new IllegalArgumentException( String.format( "Missing value for parameter '%s", args[i] ) );
                }
                configurationBuilder.set( setting, args[i] );
            }
            else
            {
                return Arrays.copyOfRange( args, i, args.length );
            }
        }
        return new String[0];
    }

    public static Parameters parser( Setting<?>... settings )
    {
        return new Parameters( settings );
    }

    private final Map<String, Setting<?>> settings = new HashMap<String, Setting<?>>();

    private Parameters( Setting<?>[] settings )
    {
        for ( Setting<?> setting : settings )
        {
            this.settings.put( setting.name(), setting );
        }
    }
}
