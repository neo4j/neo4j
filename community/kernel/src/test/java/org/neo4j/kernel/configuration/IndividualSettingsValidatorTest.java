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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.util.Map;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.logging.Log;

import static java.util.Collections.singletonList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.strict_config_validation;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Settings.FALSE;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class IndividualSettingsValidatorTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();
    private Log log;

    @Before
    public void setup()
    {
        log = mock( Log.class );
    }

    @Test
    public void nonStrictRetainsSettings()
    {
        IndividualSettingsValidator iv = new IndividualSettingsValidator( singletonList( strict_config_validation ), true );

        final Map<String,String> rawConfig = stringMap( strict_config_validation.name(), FALSE,
                "dbms.jibber.jabber", "bla",
                "external_plugin.foo", "bar" );

        Config config = mockConfig( rawConfig );

        iv.validate( config, log );

        verify( log ).warn( "Unknown config option: %s", "dbms.jibber.jabber" );
        verifyNoMoreInteractions( log );
    }

    @Test
    public void strictErrorsOnUnknownSettingsInOurNamespace()
    {
        IndividualSettingsValidator iv = new IndividualSettingsValidator( singletonList( strict_config_validation ), true );

        final Map<String,String> rawConfig = stringMap( strict_config_validation.name(), TRUE,
                "dbms.jibber.jabber", "bla",
                "external_plugin.foo", "bar" );

        Config config = mockConfig( rawConfig );

        expected.expect( InvalidSettingException.class );
        expected.expectMessage( String.format( "Unknown config option 'dbms.jibber.jabber'. To resolve either remove" +
                " it from your configuration or set '%s' to false.", strict_config_validation.name() ) );

        iv.validate( config, log );
    }

    @Test
    public void strictAllowsStuffOutsideOurNamespace()
    {
        IndividualSettingsValidator iv = new IndividualSettingsValidator( singletonList( strict_config_validation ), true );

        final Map<String,String> rawConfig = stringMap( strict_config_validation.name(), TRUE,
                "external_plugin.foo", "bar" );

        Config config = mockConfig( rawConfig );

        iv.validate( config, log );
        verifyNoMoreInteractions( log );
    }

    private Config mockConfig( Map<String,String> rawConfig )
    {
        Config config = Mockito.mock( Config.class );

        when( config.getRaw() ).thenReturn( rawConfig );
        when( config.get( strict_config_validation ) )
                .thenReturn( Boolean.valueOf( rawConfig.get( strict_config_validation.name() ) ) );

        return config;
    }
}
