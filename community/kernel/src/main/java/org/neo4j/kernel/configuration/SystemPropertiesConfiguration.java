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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.helpers.collection.Iterables;

/**
 * Collect settings from System.getProperties(). For the given settings classes, using the GraphDatabaseSetting pattern,
 * check if the individual settings are available as system properties, and if so, add them to the given map.
 */
public class SystemPropertiesConfiguration
{
    private Iterable<Class<?>> settingsClasses;

    public SystemPropertiesConfiguration(Class<?>... settingsClasses)
    {
        this( Arrays.asList( settingsClasses ));
    }

    public SystemPropertiesConfiguration( Iterable<Class<?>> settingsClasses )
    {
        this.settingsClasses = settingsClasses;
    }

    public Map<String,String> apply(Map<String,String> config )
    {
        Map<String,String> systemProperties = new HashMap<String, String>(  );
        for( Map.Entry<Object, Object> prop : System.getProperties().entrySet() )
        {
            String key = (String) prop.getKey();
            for( Class<?> settingsClass : settingsClasses )
            {
                for( Field field : settingsClass.getFields() )
                {
                    try
                    {
                        GraphDatabaseSetting setting = (GraphDatabaseSetting) field.get( null );
                        if (setting.name().equals( key ))
                        {
                            setting.validate( (String) prop.getValue() );
                            systemProperties.put( key, (String) prop.getValue() );
                        }
                    }
                    catch( Throwable e )
                    {
                        // Ignore
                    }
                }
            }
        }
        systemProperties.putAll( config );
        return systemProperties;
    }
}
