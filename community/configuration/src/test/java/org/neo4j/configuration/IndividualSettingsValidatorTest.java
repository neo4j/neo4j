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
package org.neo4j.configuration;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.logging.Log;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.TRUE;

class IndividualSettingsValidatorTest
{
    private final Log log = Mockito.mock( Log.class );

    @Test
    void nonStrictRetainsSettings()
    {
        IndividualSettingsValidator iv = new IndividualSettingsValidator( singletonList( GraphDatabaseSettings.strict_config_validation ), true );

        final Map<String, String> rawConfig = MapUtil.stringMap( GraphDatabaseSettings.strict_config_validation.name(), FALSE,
                "dbms.jibber.jabber", "bla",
                "external_plugin.foo", "bar" );

        Config config = mockConfig( rawConfig );

        iv.validate( config, log );

        Mockito.verify( log ).warn( "Unknown config option: %s", "dbms.jibber.jabber" );
        Mockito.verifyNoMoreInteractions( log );
    }

    @Test
    void strictErrorsOnUnknownSettingsInOurNamespace()
    {
        IndividualSettingsValidator iv = new IndividualSettingsValidator( singletonList( GraphDatabaseSettings.strict_config_validation ), true );

        final Map<String, String> rawConfig = MapUtil.stringMap( GraphDatabaseSettings.strict_config_validation.name(), TRUE,
                "dbms.jibber.jabber", "bla",
                "external_plugin.foo", "bar" );

        Config config = mockConfig( rawConfig );

        InvalidSettingException exception = assertThrows( InvalidSettingException.class, () -> iv.validate( config, log ) );
        assertEquals(
                String.format( "Unknown config option 'dbms.jibber.jabber'. To resolve either remove" + " it from your configuration or set '%s' to false.",
                        GraphDatabaseSettings.strict_config_validation.name() ), exception.getMessage() );
    }

    @Test
    void strictAllowsStuffOutsideOurNamespace()
    {
        IndividualSettingsValidator iv = new IndividualSettingsValidator( singletonList( GraphDatabaseSettings.strict_config_validation ), true );

        final Map<String, String> rawConfig = MapUtil.stringMap( GraphDatabaseSettings.strict_config_validation.name(), TRUE,
                "external_plugin.foo", "bar" );

        Config config = mockConfig( rawConfig );

        iv.validate( config, log );
        Mockito.verifyNoMoreInteractions( log );
    }

    private Config mockConfig( Map<String,String> rawConfig )
    {
        Config config = Mockito.mock( Config.class );

        Mockito.when( config.getRaw() ).thenReturn( rawConfig );
        Mockito.when( config.get( GraphDatabaseSettings.strict_config_validation ) )
                .thenReturn( Boolean.valueOf( rawConfig.get( GraphDatabaseSettings.strict_config_validation.name() ) ) );

        return config;
    }
}
