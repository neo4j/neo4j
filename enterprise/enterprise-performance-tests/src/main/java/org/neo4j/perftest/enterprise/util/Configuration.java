/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

        private final Map<String, String> config;

        public Configuration build()
        {
            return fromMap( new HashMap<>( config ) );
        }

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
