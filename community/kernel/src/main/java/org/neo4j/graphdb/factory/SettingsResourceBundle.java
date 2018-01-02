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
package org.neo4j.graphdb.factory;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.ResourceBundle;
import java.util.Set;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Functions;
import org.neo4j.kernel.configuration.Internal;
import org.neo4j.kernel.configuration.Obsoleted;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;

/**
 * ResourceBundle for classes that use GraphDatabaseSetting, which use reflection to find its values.
 * <p>
 * This allows us to keep the descriptions in the Java code, so they are available in JavaDoc.
 */
public class SettingsResourceBundle
        extends ResourceBundle
{
    public static final String MANDATORY = ".mandatory";
    public static final String DEFAULT = ".default";
    public static final String INTERNAL = ".internal";
    public static final String VALIDATIONMESSAGE = ".validationmessage";
    public static final String OBSOLETED = ".obsoleted";
    public static final String DEPRECATED = ".deprecated";
    public static final String DESCRIPTION = ".description";
    public static final String CLASS_DESCRIPTION = "description";
    private final Class<?> settingsClass;

    public SettingsResourceBundle( Class settingsClass )
    {
        this.settingsClass = settingsClass;
    }

    @Override
    protected Object handleGetObject( String key )
    {
        if ( key.equals( CLASS_DESCRIPTION ) )
        {
            Description description = (Description) settingsClass.getAnnotation( Description.class );
            return description.value();
        }

        String name = key.substring( 0, key.lastIndexOf( "." ) );

        if ( key.endsWith( DESCRIPTION ) )
        {
            Field settingField = getField( name );
            return settingField.getAnnotation( Description.class ).value();
        }

        if ( key.endsWith( DEPRECATED ) )
        {
            return "The " + name + " configuration setting has been deprecated.";
        }

        if ( key.endsWith( INTERNAL ) )
        {
            return "The " + name + " configuration setting is for internal use.";
        }

        if ( key.endsWith( OBSOLETED ) )
        {
            Field settingField = getField( name );
            return settingField.getAnnotation( Obsoleted.class ).value();
        }

        if ( key.endsWith( VALIDATIONMESSAGE ) )
        {
            try
            {
                Field settingField = getField( name );
                Setting<?> setting = (Setting<?>) settingField.get( null );
                return setting.toString();
            }
            catch ( Exception e )
            {
                // Ignore
            }
        }

        if ( key.endsWith( DEFAULT ) )
        {
            Field settingField = getField( name );
            try
            {
                Setting<?> setting = (Setting<?>) settingField.get( null );
                return setting.getDefaultValue().toString();
            }
            catch ( Exception e )
            {
                // Ignore
            }
        }

        if ( key.endsWith( MANDATORY ) )
        {
            return "The " + name + " configuration setting is mandatory.";
        }

        throw new IllegalResourceException( "Could not find resource for property " + key );
    }

    private Setting<?> getFieldValue( String name ) throws IllegalAccessException
    {
        return (Setting<?>) getField( name ).get( null );
    }

    private Field getField( String name )
    {
        for ( Field field : settingsClass.getFields() )
        {
            if ( Setting.class.isAssignableFrom( field.getType() ) )
            {
                try
                {
                    Setting setting = (Setting) field.get( null );
                    if ( setting.name().equals( name ) )
                    {
                        return field;
                    }
                }
                catch ( Exception e )
                {
                    // Ignore
                }
            }
        }
        throw new IllegalResourceException( "Could not find resource for property with prefix " + name );
    }

    @Override
    public Enumeration<String> getKeys()
    {
        return Collections.enumeration( keySet() );
    }

    @Override
    public Set<String> keySet()
    {
        Set<String> keys = new LinkedHashSet<String>();

        {
            Description description = (Description) settingsClass.getAnnotation( Description.class );
            if ( description != null )
            {
                keys.add( CLASS_DESCRIPTION );
            }
        }

        for ( Field field : settingsClass.getFields() )
        {
            Setting<?> setting = null;
            try
            {
                setting = (Setting<?>) field.get( null );
            }
            catch ( Exception e )
            {
                continue;
            }
            String name = setting.name();
            if ( field.getAnnotation( Internal.class ) != null )
            {
                keys.add( name + INTERNAL );
            }
            if ( field.getAnnotation( Description.class ) != null )
            {
                keys.add( name + DESCRIPTION );
                keys.add( name + VALIDATIONMESSAGE );
                Object defaultValue = null;
                try
                {
                    defaultValue = setting.apply( Functions.<String, String>nullFunction() );
                }
                catch ( IllegalArgumentException iae )
                {
                    if ( iae.toString().indexOf( "mandatory" ) != -1 )
                    {
                        keys.add( name + MANDATORY );
                    }
                }
                if ( defaultValue != null )
                {
                    keys.add( name + DEFAULT );
                }
            }
            else
            {
                System.out.println( "Missing description for: " + field.getName() );
            }
            if ( field.getAnnotation( Deprecated.class ) != null )
            {
                keys.add( name + DEPRECATED );
            }
            if ( field.getAnnotation( Obsoleted.class ) != null )
            {
                keys.add( name + OBSOLETED );
            }
        }
        return keys;
    }
}
