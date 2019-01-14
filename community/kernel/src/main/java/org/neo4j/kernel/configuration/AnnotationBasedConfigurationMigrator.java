/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

import org.neo4j.configuration.LoadableConfig;
import org.neo4j.logging.Log;

public class AnnotationBasedConfigurationMigrator implements ConfigurationMigrator
{
    private ArrayList<ConfigurationMigrator> migrators = new ArrayList<>();

    AnnotationBasedConfigurationMigrator( @Nonnull Iterable<LoadableConfig> settingsClasses )
    {
        for ( LoadableConfig loadableConfig : settingsClasses )
        {
            for ( ConfigurationMigrator field : getMigratorsFromClass( loadableConfig.getClass() ) )
            {
                migrators.add( field );
            }
        }
    }

    @Override
    @Nonnull
    public Map<String,String> apply( @Nonnull Map<String,String> rawConfiguration, @Nonnull Log log )
    {
        for ( ConfigurationMigrator migrator : migrators )
        {
            rawConfiguration = migrator.apply( rawConfiguration, log );
        }
        return rawConfiguration;
    }

    /**
     * Find all {@link ConfigurationMigrator} annotated with {@link Migrator} from a given class.
     *
     * @param clazz The class to scan for migrators.
     * @throws AssertionError if a field annotated as a {@link Migrator} is not static or does not implement
     * {@link ConfigurationMigrator}.
     */
    private static Iterable<ConfigurationMigrator> getMigratorsFromClass( Class<?> clazz )
    {
        List<ConfigurationMigrator> found = new ArrayList<>();
        for ( Field field : clazz.getDeclaredFields() )
        {
            if ( field.isAnnotationPresent( Migrator.class ) )
            {
                if ( !ConfigurationMigrator.class.isAssignableFrom( field.getType() ) )
                {
                    throw new AssertionError( "Field annotated as Migrator has to implement ConfigurationMigrator" );
                }

                if ( !Modifier.isStatic( field.getModifiers() ) )
                {
                    throw new AssertionError( "Field annotated as Migrator has to be static" );
                }

                try
                {
                    field.setAccessible( true );
                    found.add( (ConfigurationMigrator) field.get( null ) );
                }
                catch ( IllegalAccessException ex )
                {
                    throw new AssertionError( "Field annotated as Migrator could not be accessed", ex );
                }
            }
        }

        return found;
    }
}
