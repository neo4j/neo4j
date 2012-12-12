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
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Iterables;

/**
 * Provides defaults for database settings.
 */
public class ConfigurationDefaults
{
	private static AnnotatedFieldHarvester fieldHarvester = new AnnotatedFieldHarvester();
	
    @SuppressWarnings("rawtypes")
	public static String getDefault(GraphDatabaseSetting<?> realSetting, Class<?> settingsClass)
    {
        for( Pair<Field, GraphDatabaseSetting> field : fieldHarvester.findStatic(settingsClass, GraphDatabaseSetting.class) )
        {
            if (field.other() == realSetting)
            {
                if (field.other() instanceof GraphDatabaseSetting.DefaultValue)
                {
                    return ((GraphDatabaseSetting.DefaultValue)field.other()).getDefaultValue();
                } else
                {
                    return getDefaultValue( field.first() );
                }
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
        this( Iterables.<Class<?>,Class<?>>iterable( settingsClasses ));
    }

    public ConfigurationDefaults( Iterable<Class<?>> settingsClasses )
    {
        this.settingsClasses = settingsClasses;
    }

    @SuppressWarnings("rawtypes")
	public Map<String,String> apply(Map<String,String> config)
    {
        Map<String, String> configuration = new HashMap<String,String>(config);
        
        // Go through all settings and apply defaults
        for( Class<?> settingsClass : settingsClasses )
        {
        	for( Pair<Field, GraphDatabaseSetting> field : fieldHarvester.findStatic(settingsClass, GraphDatabaseSetting.class) )
            {
        		String defaultValue;
        		GraphDatabaseSetting setting = field.other();
                if (setting instanceof GraphDatabaseSetting.DefaultValue)
                {
                	defaultValue = ((GraphDatabaseSetting.DefaultValue)setting).getDefaultValue();
                } else
                {
                	defaultValue = getDefaultValue( field.first() );
                }

                if (defaultValue != null && !configuration.containsKey(setting.name()))
                {
                    configuration.put( setting.name(), defaultValue );
                }
            }
        }

        return configuration;
    }
    
}
