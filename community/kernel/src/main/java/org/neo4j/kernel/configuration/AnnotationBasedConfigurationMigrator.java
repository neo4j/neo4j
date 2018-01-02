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
import java.util.ArrayList;
import java.util.Map;

import org.neo4j.helpers.Pair;
import org.neo4j.logging.Log;


public class AnnotationBasedConfigurationMigrator implements ConfigurationMigrator {

    private ArrayList<ConfigurationMigrator> migrators = new ArrayList<>();
    private AnnotatedFieldHarvester fieldHarvester = new AnnotatedFieldHarvester();
    
    public AnnotationBasedConfigurationMigrator(
            Iterable<Class<?>> settingsClasses)
    {
        for( Class<?> settingsClass : settingsClasses )
        {
            for( Pair<Field,ConfigurationMigrator> field : fieldHarvester.findStatic(settingsClass, ConfigurationMigrator.class, Migrator.class) )
            {
                migrators.add(field.other());
            }
        }
    }

    @Override
    public Map<String, String> apply(Map<String, String> rawConfiguration,
            Log log)
    {
        for(ConfigurationMigrator migrator : migrators)
        {
            rawConfiguration = migrator.apply(rawConfiguration, log);
        }
        return rawConfiguration;
    }

}
