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
package org.neo4j.kernel.configuration;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Functions;

/**
 * Collect settings from System.getProperties(). For the given settings classes, using the Setting pattern,
 * check if the individual settings are available as system properties, and if so, add them to the given map.
 */
public class SystemPropertiesConfiguration
{
    private final Iterable<Class<?>> settingsClasses;

    public SystemPropertiesConfiguration( Class<?>... settingsClasses )
    {
        this( Arrays.asList( settingsClasses ) );
    }

    public SystemPropertiesConfiguration( Iterable<Class<?>> settingsClasses )
    {
        this.settingsClasses = settingsClasses;
    }

    public Map<String,String> apply( Map<String,String> config )
    {
        // Create test config with base plus system props on top
        Map<String,String> systemProperties = new HashMap<>( config );
        for ( Map.Entry<Object,Object> prop : System.getProperties().entrySet() )
        {
            systemProperties.put( prop.getKey().toString(), prop.getValue().toString() );
        }
        // For each system property, see if it passes validation
        // If so, add it to result set
        Map<String,String> result = new HashMap<String,String>( config );
        Function<String,String> systemPropertiesFunction = Functions.map( systemProperties );
        for ( Map.Entry<Object,Object> prop : System.getProperties().entrySet() )
        {
            String key = (String) prop.getKey();
            for ( Class<?> settingsClass : settingsClasses )
            {
                for ( Field field : settingsClass.getFields() )
                {
                    try
                    {
                        Setting<Object> setting = (Setting<Object>) field.get( null );
                        if ( setting.name().equals( key ) )
                        {
                            setting.apply( systemPropertiesFunction );
                            // Valid setting, copy it from system properties
                            result.put( key, (String) prop.getValue() );
                        }
                    }
                    catch ( Throwable e )
                    {
                        continue;
                    }
                }
            }
        }
        return result;
    }
}
