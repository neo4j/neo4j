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

package org.neo4j.kernel.configuration;

import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.factory.Default;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.helpers.collection.Iterables;

/**
 * Provides defaults for database settings.
 */
public class ConfigurationDefaults
{
    public static String getDefault(GraphDatabaseSetting<?> realSetting, Class<?> settingsClass)
    {
        for( Field field : settingsClass.getFields() )
        {
            try
            {
                Object fieldValue = field.get( null );
                if(fieldValue instanceof GraphDatabaseSetting<?>) 
                {
                    GraphDatabaseSetting<?> setting = (GraphDatabaseSetting<?>) fieldValue;
                    if (setting == realSetting)
                    {
                        if (setting instanceof GraphDatabaseSetting.DefaultValue)
                        {
                            return ((GraphDatabaseSetting.DefaultValue)setting).getDefaultValue();
                        } else
                        {
                            return getDefaultValue( field );
                        }
                    }
                }
            }
            catch( IllegalAccessException e )
            {
                assert false : "Field "+field.getName()+" is not public";
            }
        }
        throw new IllegalArgumentException( MessageFormat.format("Setting {0} not found in settings-class {1}", realSetting.name(), settingsClass.getName() ));
    }

    private static String getDefaultValue(Field field)
    {
        Default defaultAnnotation = field.getAnnotation( Default.class );
        if (defaultAnnotation == null)
        {
            return null;
        }

        return defaultAnnotation.value();
    }

    private Iterable<Class<?>> settingsClasses;

    public ConfigurationDefaults(Class<?>... settingsClasses)
    {
        this( Iterables.iterable( settingsClasses ));
    }

    public ConfigurationDefaults( Iterable<Class<?>> settingsClasses )
    {
        this.settingsClasses = settingsClasses;
    }

    public Map<String,String> apply(Map<String,String> config)
    {
        Map<String, String> configuration = new HashMap<String,String>(config);
        
        // Go through all settings and apply defaults
        for( Class<?> settingsClass : settingsClasses )
        {
            for( Field field : settingsClass.getFields() )
            {
                try
                {
                    Object fieldValue = field.get( null );
                    if(fieldValue instanceof GraphDatabaseSetting<?>) 
                    {
                        GraphDatabaseSetting<?> setting = (GraphDatabaseSetting<?>) fieldValue;
                        if (!configuration.containsKey( setting.name() ))
                        {
                            if (setting instanceof GraphDatabaseSetting.DefaultValue)
                            {
                                String defaultValue = ((GraphDatabaseSetting.DefaultValue)setting).getDefaultValue();
                                if (defaultValue != null)
                                    configuration.put( setting.name(), defaultValue );
                            } else
                            {
                                String defaultValue = getDefaultValue( field );
                                if (defaultValue != null)
                                    configuration.put( setting.name(), defaultValue );
                            }
                        }
                    }
                }
                catch( IllegalAccessException e )
                {
                    assert false : "Field "+field.getName()+" is not public";
                }
            }
        }

        return configuration;
    }
    
}