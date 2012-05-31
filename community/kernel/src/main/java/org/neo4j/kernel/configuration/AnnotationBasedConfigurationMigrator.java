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
import java.util.ArrayList;
import java.util.Map;

import org.neo4j.graphdb.factory.Migrator;
import org.neo4j.kernel.impl.util.StringLogger;


public class AnnotationBasedConfigurationMigrator implements ConfigurationMigrator {

    private ArrayList<ConfigurationMigrator> migrators = new ArrayList<ConfigurationMigrator>();
    
    public AnnotationBasedConfigurationMigrator(
            Iterable<Class<?>> settingsClasses)
    {
        for( Class<?> settingsClass : settingsClasses )
        {
            for( Field field : settingsClass.getFields() )
            {
                try
                {
                    Object fieldValue = field.get( null );
                    if(fieldValue instanceof ConfigurationMigrator && field.getAnnotation(Migrator.class) != null) 
                    {
                        migrators.add((ConfigurationMigrator)fieldValue);
                    }
                } catch( IllegalAccessException e )
                {
                    assert false : "Field "+field.getName()+" is not public";
                } catch( NullPointerException npe )
                {
                    throw new IllegalArgumentException(settingsClass.getName() + "#" + field.getName() + " needs to be static to be used as configuration meta data.");
                }
            }
        }
    }

    @Override
    public Map<String, String> apply(Map<String, String> rawConfiguration,
            StringLogger log)
    {
        for(ConfigurationMigrator migrator : migrators)
        {
            rawConfiguration = migrator.apply(rawConfiguration, log);
        }
        return rawConfiguration;
    }

}