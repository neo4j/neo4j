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

import java.util.Map;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.logging.Log;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.strict_config_validation;
import static org.neo4j.configuration.Settings.FALSE;
import static org.neo4j.configuration.Settings.TRUE;

class IndividualSettingsValidatorTest
{
    private final Log log = mock( Log.class );

    @Test
    void nonStrictRetainsSettings()
    {
        IndividualSettingsValidator iv = new IndividualSettingsValidator( singletonList( strict_config_validation ), true );

        final Map<String, String> rawConfig = MapUtil.stringMap( strict_config_validation.name(), FALSE,
                "dbms.jibber.jabber", "bla",
                "external_plugin.foo", "bar" );

        Config config = mockConfig( rawConfig );

        iv.validate( config, log );

        verify( log ).warn( "Unknown config option: %s", "dbms.jibber.jabber" );
        verifyNoMoreInteractions( log );
    }

    @Test
    void strictErrorsOnUnknownSettingsInOurNamespace()
    {
        IndividualSettingsValidator iv = new IndividualSettingsValidator( singletonList( strict_config_validation ), true );

        final Map<String, String> rawConfig = MapUtil.stringMap( strict_config_validation.name(), TRUE,
                "dbms.jibber.jabber", "bla",
                "external_plugin.foo", "bar" );

        Config config = mockConfig( rawConfig );

        InvalidSettingException exception = assertThrows( InvalidSettingException.class, () -> iv.validate( config, log ) );
        assertEquals(
                String.format( "Unknown config option 'dbms.jibber.jabber'. To resolve either remove" + " it from your configuration or set '%s' to false.",
                        strict_config_validation.name() ), exception.getMessage() );
    }

    @Test
    void strictAllowsStuffOutsideOurNamespace()
    {
        IndividualSettingsValidator iv = new IndividualSettingsValidator( singletonList( strict_config_validation ), true );

        final Map<String, String> rawConfig = MapUtil.stringMap( strict_config_validation.name(), TRUE,
                "external_plugin.foo", "bar" );

        Config config = mockConfig( rawConfig );

        iv.validate( config, log );
        verifyNoMoreInteractions( log );
    }

    private Config mockConfig( Map<String,String> rawConfig )
    {
        Config config = mock( Config.class );

        when( config.getRaw() ).thenReturn( rawConfig );
        when( config.get( strict_config_validation ) )
                .thenReturn( Boolean.valueOf( rawConfig.get( strict_config_validation.name() ) ) );

        return config;
    }
}
