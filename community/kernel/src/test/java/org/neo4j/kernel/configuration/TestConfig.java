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

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class TestConfig
{

    public static class MyMigratingSettings
    {
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

    public static class MySettingsWithDefaults
    {
        public static Setting<String> hello = setting( "hello", STRING, "Hello, World!" );

        public static Setting<Boolean> boolSetting = setting( "bool_setting", BOOLEAN, Settings.TRUE );

    }

    private class ChangeCaptureListener implements ConfigurationChangeListener
    {
        private Set<ConfigurationChange> lastChangeSet;

        @Override
        public void notifyConfigurationChanges( Iterable<ConfigurationChange> change )
        {
            lastChangeSet = new HashSet<>();
            for ( ConfigurationChange ch : change )
            {
                lastChangeSet.add( ch );
            }
        }
    }

    private <T> Set<T> setOf( T... objs )
    {
        Set<T> set = new HashSet<>();
        Collections.addAll( set, objs );
        return set;
    }

    @Test
    public void shouldApplyDefaults()
    {
        Config config = new Config( new HashMap<String, String>(), MySettingsWithDefaults.class );

        assertThat( config.get( MySettingsWithDefaults.hello ), is( "Hello, World!" ) );
    }

    @Test
    public void shouldApplyMigrations()
    {
        // When
        Config config = new Config( stringMap("old", "hello!"), MyMigratingSettings.class );

        // Then
        assertThat( config.get( MyMigratingSettings.newer ), is( "hello!" ) );
    }

    @Test( expected = InvalidSettingException.class )
    public void shouldNotAllowSettingInvalidValues()
    {
        Config config = new Config( new HashMap<String, String>(), MySettingsWithDefaults.class );

        Map<String, String> params = config.getParams();
        params.put( MySettingsWithDefaults.boolSetting.name(), "asd" );

        config.applyChanges( params );

        fail( "Expected validation to fail." );
    }

    @Test( expected = InvalidSettingException.class )
    public void shouldNotAllowInvalidValuesInConstructor()
    {
        new Config( stringMap( MySettingsWithDefaults.boolSetting.name(), "asd" ), MySettingsWithDefaults.class );

        fail( "Expected validation to fail." );
    }

    @Test
    public void shouldNotifyChangeListenersWhenNewSettingsAreApplied()
    {
        // Given
        Config config = new Config( stringMap("setting", "old"), MyMigratingSettings.class );
        ChangeCaptureListener listener = new ChangeCaptureListener();
        config.addConfigurationChangeListener( listener );

        // When
        config.applyChanges( stringMap( "setting", "new" ) );

        // Then
        assertThat( listener.lastChangeSet,
                is( setOf( new ConfigurationChange( "setting", "old", "new" ) ) ) );
    }

    @Test
    public void shouldNotNotifyChangeListenerWhenNothingChanged()
    {
        // Given
        Config config = new Config( stringMap("setting", "old"), MyMigratingSettings.class );
        ChangeCaptureListener listener = new ChangeCaptureListener();
        config.addConfigurationChangeListener( listener );

        // When
        config.applyChanges( stringMap( "setting", "old" ) ); // nothing really changed here

        // Then
        assertThat( listener.lastChangeSet, nullValue() );
    }
    
    @Test
    public void settingNewPropertyMustNotAlterExistingSettings()
    {
        // Given
        Config config = new Config( stringMap( "a", "1" ) );

        // When
        config.setProperty( "b", "2" );

        // Then
        assertThat( config.getParams(), is( stringMap( "a", "1", "b", "2" ) ) );
    }

    @Test
    public void shouldBeAbleToRegisterSettingsClassesAfterInstantiation() throws Exception
    {
        // Given
        Config config = new Config( stringMap(  "old", "hello!" ) );

        // When
        config.registerSettingsClasses( asList( MySettingsWithDefaults.class, MyMigratingSettings.class ) );

        // Then
        assertThat( config.get( MyMigratingSettings.newer ), equalTo( "hello!" ) );
        assertThat( config.get( MySettingsWithDefaults.hello ), equalTo( "Hello, World!" ) );
    }

    @Test
    public void shouldBeAbleToAgumentConfig() throws Exception
    {
        // Given
        Config config = new Config( stringMap( "newer", "old", "non-overlapping", "huzzah" ) );

        // When
        config.augment( stringMap( "newer", "new", "unrelated", "hello" ) );

        // Then
        assertThat( config.get( setting("newer", STRING, "") ), equalTo( "new" ) );
        assertThat( config.get( setting("non-overlapping", STRING, "") ), equalTo( "huzzah" ) );
        assertThat( config.get( setting("unrelated", STRING, "") ), equalTo( "hello" ) );
    }
}
