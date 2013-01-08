/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.kernel.impl.transaction.IllegalResourceException;

/**
 * ResourceBundle for classes that use GraphDatabaseSetting, which use reflection to find its values.
 * <p/>
 * This allows us to keep the descriptions in the Java code, so they are available in JavaDoc.
 */
public class SettingsResourceBundle
        extends ResourceBundle
{
    private final Class settingsClass;

    public SettingsResourceBundle( Class settingsClass )
    {
        this.settingsClass = settingsClass;
    }

    @Override
    protected Object handleGetObject( String key )
    {
        if ( key.equals( "description" ) )
        {
            Description description = (Description) settingsClass.getAnnotation( Description.class );
            return description.value();
        }

        String name = key.substring( 0, key.lastIndexOf( "." ) );

/*        if ( key.contains( ".option." ) )
        {
            String name = key.substring( 0, key.lastIndexOf( ".option." ) );
            String option = key.substring( key.lastIndexOf( ".option." ) + ".option.".length() );

            Field settingField = getField( name );
            StringBuffer optionsBuilder = new StringBuffer();
            try
            {
                GraphDatabaseSetting.OptionsSetting optionsSetting = (GraphDatabaseSetting.OptionsSetting)
                        settingField.get( null );
                Field optionField = findOptionField( option, optionsSetting.getClass() );
                Description description = optionField.getAnnotation( Description.class );
                if ( description != null )
                {
                    return description.value();
                }
                throw new IllegalResourceException( "Could not find resource for property " + key );
            }
            catch ( Exception e )
            {
                // Ignore
            }
        }

        if ( key.endsWith( ".validationmessage" ) )
        {
            Field settingField = getField( name );
            StringBuffer optionsBuilder = new StringBuffer();
            try
            {
                GraphDatabaseSetting.OptionsSetting optionsSetting = (GraphDatabaseSetting.OptionsSetting)
                        settingField.get( null );
            }
            catch ( Exception ex )
            {
                // Ignore
            }
        }

*/

        if ( key.endsWith( ".description" ) )
        {
            Field settingField = getField( name );
            return settingField.getAnnotation( Description.class ).value();
        }

        if ( key.endsWith( ".title" ) )
        {
            Field settingField = getField( name );
            Title annotation = settingField.getAnnotation( Title.class );
            if ( annotation != null )
            {
                return annotation.value();
            }
            else
            {
                // read_only -> Read only
                name = name.replace( '_', ' ' );
                name = name.substring( 0, 1 ).toUpperCase() + name.substring( 1 );
                return name;
            }
        }

        if ( key.endsWith( ".default" ) )
        {
            Field settingField = getField( name );
            try
            {
                return ((Setting) settingField.get( null )).apply( Functions.<String,
                        String>nullFunction() ).toString();
            }
            catch ( Exception e )
            {
                // Ignore
            }
        }

/*        if ( key.endsWith( ".options" ) )
        {
            Field settingField = getField( name );
            StringBuffer optionsBuilder = new StringBuffer();
            try
            {
                for ( String option : ((GraphDatabaseSetting.OptionsSetting) settingField.get( null )).options() )
                {
                    if ( optionsBuilder.length() > 0 )
                    {
                        optionsBuilder.append( ',' );
                    }
                    optionsBuilder.append( option );
                }
                return optionsBuilder.toString();
            }
            catch ( Exception e )
            {
                // Ignore
            }
        }

        if ( key.endsWith( ".min" ) )
        {
            try
            {
                Field settingField = getField( name );
                return ((GraphDatabaseSetting.NumberSetting) settingField.get( null )).getMin().toString();
            }
            catch ( IllegalAccessException e )
            {
                // Ignore
            }
        }

        if ( key.endsWith( ".max" ) )
        {
            try
            {
                Field settingField = getField( name );
                return ((GraphDatabaseSetting.NumberSetting) settingField.get( null )).getMax().toString();
            }
            catch ( IllegalAccessException e )
            {
                // Ignore
            }
        }*/

        throw new IllegalResourceException( "Could not find resource for property " + key );
    }

    private Field getField( String name )
    {
        for ( Field field : settingsClass.getFields() )
        {
            if ( GraphDatabaseSetting.class.isAssignableFrom( field.getType() ) )
            {
                try
                {
                    GraphDatabaseSetting setting = (GraphDatabaseSetting) field.get( null );
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
                keys.add( "description" );
            }
        }

        for ( Field field : settingsClass.getFields() )
        {
            try
            {
                GraphDatabaseSetting setting = (GraphDatabaseSetting) field.get( null );
                if ( field.getAnnotation( Description.class ) != null )
                {
                    keys.add( setting.name() + ".description" );
//                    keys.add( setting.name() + ".title" );
                    if ( setting.apply( Functions.<String, String>nullFunction() ) != null )
                    {
                        keys.add( setting.name() + ".default" );
                    }
/*
                    if ( setting instanceof GraphDatabaseSetting.OptionsSetting )
                    {
                        keys.add( setting.name() + ".options" );
                        try
                        {
                            for ( String option : ((GraphDatabaseSetting.OptionsSetting) setting).options() )
                            {
                                Field optionField = findOptionField( option, setting.getClass() );
                                Description description = optionField.getAnnotation( Description.class );
                                if ( description != null )
                                {
                                    keys.add( setting.name() + ".option." + option );
                                }
                            }
                        }
                        catch ( NoSuchFieldException e )
                        {
                        }
                    }
                    if ( setting instanceof GraphDatabaseSetting.NumberSetting )
                    {
                        GraphDatabaseSetting.NumberSetting numberSetting = (GraphDatabaseSetting.NumberSetting) setting;
                        if ( numberSetting.getMin() != null )
                        {
                            keys.add( setting.name() + ".min" );
                        }
                        if ( numberSetting.getMax() != null )
                        {
                            keys.add( setting.name() + ".max" );
                        }
                    }
*/
                }
            }
            catch ( Exception e )
            {
                // Ignore
            }
        }
        return keys;
    }

    private Field findOptionField( String option, Class<? extends GraphDatabaseSetting> optionsClass )
            throws NoSuchFieldException
    {
        for ( Field optionField : optionsClass.getFields() )
        {
            try
            {
                if ( option.equals( optionField.get( null ) ) )
                {
                    return optionField;
                }
            }
            catch ( IllegalAccessException e )
            {
                // Ignore
            }
        }
        throw new NoSuchFieldException( "No field found for option " + option );
    }
}
