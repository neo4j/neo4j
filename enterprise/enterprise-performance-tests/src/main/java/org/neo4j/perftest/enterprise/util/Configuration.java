package org.neo4j.perftest.enterprise.util;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.reflect.Modifier.isStatic;

public abstract class Configuration
{
    public static final Configuration SYSTEM_PROPERTIES = new Configuration()
    {
        @Override
        protected String getConfiguration( String name )
        {
            return System.getProperty( name );
        }
    };

    public static Configuration combine( Configuration first, Configuration other, Configuration... more )
    {
        final Configuration[] configurations = new Configuration[2 + (more == null ? 0 : more.length)];
        configurations[0] = first;
        configurations[1] = other;
        if ( more != null )
        {
            System.arraycopy( more, 0, configurations, 2, more.length );
        }
        return new Configuration()
        {
            @Override
            protected String getConfiguration( String name )
            {
                for ( Configuration configuration : configurations )
                {
                    String value = configuration.getConfiguration( name );
                    if ( value != null )
                    {
                        return value;
                    }
                }
                return null;
            }
        };
    }

    public static Setting<?>[] settingsOf( Class<?>... settingsHolders )
    {
        List<Setting<?>> result = new ArrayList<Setting<?>>();
        for ( Class<?> settingsHolder : settingsHolders )
        {
            for ( Field field : settingsHolder.getDeclaredFields() )
            {
                if ( isStatic( field.getModifiers() ) && field.getType() == Setting.class )
                {
                    field.setAccessible( true );
                    try
                    {
                        result.add( (Setting) field.get( settingsHolder ) );
                    }
                    catch ( IllegalAccessException e )
                    {
                        throw new IllegalStateException( "Field should have been made accessible", e );
                    }
                }
            }
        }
        return result.toArray( new Setting<?>[result.size()] );
    }

    public static Configuration fromMap( final Map<String, String> config )
    {
        return new Configuration()
        {
            @Override
            protected String getConfiguration( String name )
            {
                return config.get( name );
            }
        };
    }

    public static final class Builder
    {
        public Configuration build()
        {
            return fromMap( new HashMap<String, String>( config ) );
        }

        private final Map<String, String> config;

        private Builder( HashMap<String, String> config )
        {
            this.config = config;
        }

        public void set( Setting<?> setting, String value )
        {
            setting.validateValue( value );
            config.put( setting.name(), value );
        }

        public <T> void setValue( Setting<T> setting, T value )
        {
            set( setting, setting.asString( value ) );
        }
    }

    public <T> T get( Setting<T> setting )
    {
        String value = getConfiguration( setting.name() );
        if ( value == null )
        {
            return setting.defaultValue();
        }
        return setting.parse( value );
    }

    protected abstract String getConfiguration( String name );

    public static Builder builder()
    {
        return new Builder( new HashMap<String, String>() );
    }
}
