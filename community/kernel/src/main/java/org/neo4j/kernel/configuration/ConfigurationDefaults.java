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
import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.factory.Default;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Provides defaults for database settings.
 */
public class ConfigurationDefaults
{
    private StringLogger msgLog;
    private Iterable<Class<?>> settingsClasses;

    public ConfigurationDefaults( StringLogger msgLog, Iterable<Class<?>> settingsClasses )
    {
        this.msgLog = msgLog;
        this.settingsClasses = settingsClasses;
    }

    public Map<String,String> apply(Map<String,String> config)
    {
        Map<String, String> configuration = new HashMap<String,String>(config);
        
        // Go through all settings and apply defaults
        for( Class settingsClass : settingsClasses )
        {
            for( Field field : settingsClass.getFields() )
            {
                try
                {
                    GraphDatabaseSetting setting = (GraphDatabaseSetting) field.get( null );
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
                catch( IllegalAccessException e )
                {
                    msgLog.logMessage( "Could not apply defaults for" );
                }
            }
        }

        return configuration;
    }
    
    private String getDefaultValue(Field field)
    {
        Default defaultAnnotation = field.getAnnotation( Default.class );
        if (defaultAnnotation == null)
        {
            return null;
        }
        
        return defaultAnnotation.value();
    }
}
