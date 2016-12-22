/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.neo4j.configuration.LoadableConfig;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.logging.Log;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

public class ConfigTest
{
    public static class MyMigratingSettings implements LoadableConfig
    {
        @SuppressWarnings("unused") // accessed by reflection
        @Migrator
        public static ConfigurationMigrator migrator = new BaseConfigurationMigrator()
        {
            {
                add( new SpecificPropertyMigration( "old", "Old has been replaced by newer!" )
                {
                    @Override
                    public void setValueWithOldSetting( String value, Map<String, String> rawConfiguration )
                    {
                        rawConfiguration.put( newer.name(), value );
                    }
                } );
            }
        };

        public static Setting<String> newer = setting( "newer", STRING, "" );
    }

    public static class MySettingsWithDefaults implements LoadableConfig
    {
        public static final Setting<String> hello = setting( "hello", STRING, "Hello, World!" );

        public static final Setting<Boolean> boolSetting = setting( "bool_setting", BOOLEAN, Settings.TRUE );

    }

    private static MyMigratingSettings myMigratingSettings = new MyMigratingSettings();
    private static MySettingsWithDefaults mySettingsWithDefaults = new MySettingsWithDefaults();

    static Config Config()
    {
        return Config( Collections.emptyMap() );
    }

    static Config Config( Map<String,String> params ) {
        return new Config( Optional.empty(), params, s -> {}, Collections.emptyList(), Optional.empty(),
                Arrays.asList(mySettingsWithDefaults, myMigratingSettings ) );
    }

    @Test
    public void shouldApplyDefaults()
    {
        Config config = Config();

        assertThat( config.get( MySettingsWithDefaults.hello ), is( "Hello, World!" ) );
    }

    @Test
    public void shouldApplyMigrations()
    {
        // When
        Config config = Config( stringMap( "old", "hello!" ) );

        // Then
        assertThat( config.get( MyMigratingSettings.newer ), is( "hello!" ) );
    }

    @Test(expected = InvalidSettingException.class)
    public void shouldNotAllowSettingInvalidValues()
    {
        Config( stringMap( MySettingsWithDefaults.boolSetting.name(), "asd" ) );
        fail( "Expected validation to fail." );
    }

    @Test
    public void shouldBeAbleToAugmentConfig() throws Exception
    {
        // Given
        Config config = Config();

        // When
        config.augment( stringMap( MySettingsWithDefaults.boolSetting.name(), Settings.FALSE ) );
        config.augment( stringMap( MySettingsWithDefaults.hello.name(), "Bye" ) );

        // Then
        assertThat( config.get( MySettingsWithDefaults.boolSetting ), equalTo( false ) );
        assertThat( config.get( MySettingsWithDefaults.hello ), equalTo( "Bye" ) );
    }

    @Test
    public void shouldRetainCustomConfigOutsideNamespaceAndPassOnBufferedLogInWithMethods() throws Exception
    {
        // Given
        Log log = mock(Log.class);
        Config first = Config.embeddedDefaults( stringMap( "first.jibberish", "bah" ) );

        // When
        first.setLogger( log );
        Config second = first.withDefaults( stringMap( "second.jibberish", "baah" ) );
        Config third = second.with( stringMap( "third.jibberish", "baaah" ) );

        // Then
        verifyNoMoreInteractions( log );
        assertEquals( Optional.of( "bah" ), third.getRaw( "first.jibberish" ) );
        assertEquals( Optional.of( "baah" ), third.getRaw( "second.jibberish" ));
        assertEquals( Optional.of( "baaah" ), third.getRaw( "third.jibberish" ) );
    }

    @Test
    public void shouldWarnAndDiscardUnknownOptionsInReserverdNamespaceAndPassOnBufferedLogInWithMethods()
            throws Exception
    {
        // Given
        Log log = mock(Log.class);
        Config first = Config.embeddedDefaults( stringMap( "dbms.jibberish", "bah" ) );

        // When
        first.setLogger( log );
        Config second = first.with( stringMap( "ha.jibberish", "baah" ) );
        second.withDefaults( stringMap( "causal_clustering.jibberish", "baah" ) );

        // Then
        verify( log ).warn( "Ignored unknown config option: %s", "dbms.jibberish" );
        verify( log ).warn( "Ignored unknown config option: %s", "ha.jibberish" );
        verify( log ).warn( "Ignored unknown config option: %s", "causal_clustering.jibberish" );
        verifyNoMoreInteractions( log );
    }

    @Test
    public void shouldPassOnValidatorsOnWithMethods() throws Exception
    {
        // Given
        ConfigurationValidator validator = spy( new ConfigurationValidator()
        {
            @Nonnull
            @Override
            public Map<String,String> validate( @Nonnull Collection<SettingValidator> settingValidators,
                    @Nonnull Map<String,String> rawConfig, @Nonnull Log log ) throws InvalidSettingException
            {
                return rawConfig;
            }
        } );

        Config first = Config.embeddedDefaults( stringMap( "first.jibberish", "bah" ),
                Collections.singleton( validator ) );

        // When
        Config second = first.withDefaults( stringMap( "second.jibberish", "baah" ) );
        second.with( stringMap( "third.jibberish", "baah" ) );

        // Then
        verify( validator, times( 3 ) ).validate( any(), any(), any() );
    }
}
