/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.ResourceBundle;
import org.neo4j.kernel.impl.transaction.IllegalResourceException;

/**
 * Default ResourceBundle for GraphDatabaseSettings that use reflection to find its value.
 *
 * This allows us to keep the descriptions in the Java code, so they are available in JavaDoc.
 */
public class GraphDatabaseSettingsResourceBundle
    extends ResourceBundle
{
    @Override
    protected Object handleGetObject( String key )
    {
        if (key.endsWith( ".description" ))
        {
            Field settingField = getField( key.split( "\\." )[ 0 ] );
            return settingField.getAnnotation( Description.class ).value();
        }

        if (key.endsWith( ".description" ))
        {
            Field settingField = getField( key.split( "\\." )[ 0 ] );
            return settingField.getAnnotation( Default.class ).value();
        }

        if (key.endsWith( ".options" ))
        {
            Field settingField = getField( key.split( "\\." )[ 0 ] );
            StringBuffer optionsBuilder = new StringBuffer(  );
            try
            {
                for( String option : ( (GraphDatabaseSetting.OptionsSetting) settingField.get( null ) ).options() )
                {
                    if (optionsBuilder.length()>0)
                        optionsBuilder.append( ',' );
                    optionsBuilder.append( option );
                }
                return optionsBuilder.toString();
            }
            catch( Exception e )
            {
                // Ignore
            }
        }

        throw new IllegalResourceException( "Could not find resource for property "+key );
    }

    private Field getField( String name )
    {
        for( Field field : GraphDatabaseSettings.class.getFields() )
        {
            try
            {
                GraphDatabaseSetting setting = (GraphDatabaseSetting) field.get( null );
                if (setting.name().equals( name ))
                    return field;
            }
            catch( Exception e )
            {
                // Ignore
            }
        }
        throw new IllegalResourceException( "Could not find resource for property" );
    }

    @Override
    public Enumeration<String> getKeys()
    {
        List<String> keys = new ArrayList<String>(  );
        for( Field field : GraphDatabaseSettings.class.getFields() )
        {
            try
            {
                GraphDatabaseSetting setting = (GraphDatabaseSetting) field.get( null );
                if (field.getAnnotation( Description.class ) != null)
                    keys.add( setting.name()+".description" );
                if (field.getAnnotation( Default.class ) != null)
                    keys.add( setting.name()+".default" );
                if (setting instanceof GraphDatabaseSetting.OptionsSetting)
                {
                    keys.add( setting.name()+".options" );
                }
            }
            catch( Exception e )
            {
                // Ignore
            }
        }
        
        return Collections.enumeration( keys );
    }
}
