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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.configuration.LoadableConfig;
import org.neo4j.logging.Log;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

public class AnnotationBasedConfigurationMigratorTest
{

    private static final AtomicBoolean wasCalled = new AtomicBoolean( false );

    static class SomeSettings implements LoadableConfig
    {
        @SuppressWarnings( "unused" )
        @Migrator
        private static ConfigurationMigrator migrator = ( rawConfiguration, log ) ->
        {
            wasCalled.set( true );
            return rawConfiguration;
        };
    }

    @Test
    public void migratorShouldGetPickedUp()
    {

        // Given
        AnnotationBasedConfigurationMigrator migrator =
                new AnnotationBasedConfigurationMigrator( Collections.singleton( new SomeSettings() ) );

        // When
        migrator.apply( new HashMap<>(), mock( Log.class ) );

        // Then
        assertThat( wasCalled.get(), is( true ) );

    }

}
