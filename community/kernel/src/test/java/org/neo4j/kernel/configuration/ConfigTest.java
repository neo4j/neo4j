/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.config.Configuration;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Settings.BOOLEAN;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;

public class ConfigTest
{
    public static class MyMigratingSettings
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

    public static class MySettingsWithDefaults
    {
        public static Setting<String> hello = setting( "hello", STRING, "Hello, World!" );

        public static Setting<Boolean> boolSetting = setting( "bool_setting", BOOLEAN, Settings.TRUE );

    }

    @Test
    public void shouldApplyDefaults()
    {
        Config config = new Config( new HashMap<>(), MySettingsWithDefaults.class );

        assertThat( config.get( MySettingsWithDefaults.hello ), is( "Hello, World!" ) );
    }

    @Test
    public void shouldApplyMigrations()
    {
        // When
        Config config = new Config( stringMap( "old", "hello!" ), MyMigratingSettings.class );

        // Then
        assertThat( config.get( MyMigratingSettings.newer ), is( "hello!" ) );
    }

    @Test(expected = InvalidSettingException.class)
    public void shouldNotAllowSettingInvalidValues()
    {
        new Config( stringMap( MySettingsWithDefaults.boolSetting.name(), "asd" ), MySettingsWithDefaults.class );
        fail( "Expected validation to fail." );
    }

    @Test
    public void shouldBeAbleToAugmentConfig() throws Exception
    {
        // Given
        Config config = new Config( stringMap( "newer", "old", "non-overlapping", "huzzah" ) );

        // When
        config.augment( stringMap( "newer", "new", "unrelated", "hello" ) );

        // Then
        assertThat( config.get( setting( "newer", STRING, "" ) ), equalTo( "new" ) );
        assertThat( config.get( setting( "non-overlapping", STRING, "" ) ), equalTo( "huzzah" ) );
        assertThat( config.get( setting( "unrelated", STRING, "" ) ), equalTo( "hello" ) );
    }

    @Test
    public void shouldProvideViewOfGroups() throws Throwable
    {
        // Given
        Config config = new Config( stringMap(
                "my.users.0.user.name", "Bob",
                "my.users.0.user.age", "81",
                "my.users.1.user.name", "Greta",
                "my.users.1.user.age", "82" ) );

        Setting<String> name = setting( "user.name", STRING, NO_DEFAULT );
        Setting<Integer> age = setting( "user.age", INTEGER, NO_DEFAULT );

        // When
        List<Configuration> views = config.view( ConfigGroups.groups( "my.users" ) );

        // Then
        assertThat( views.size(), equalTo( 2 ) );

        Configuration bob = views.get( 0 );
        assertThat( bob.get( name ), equalTo( "Bob" ) );
        assertThat( bob.get( age ), equalTo( 81 ) );

        Configuration greta = views.get( 1 );
        assertThat( greta.get( name ), equalTo( "Greta" ) );
        assertThat( greta.get( age ), equalTo( 82 ) );

        // however given the full name, the config could still be accessed outside the group
        Setting<String> name0 = setting( "my.users.0.user.name", STRING, NO_DEFAULT );
        assertThat( config.get( name0 ), equalTo( "Bob" ) );

    }

    @Test
    public void shouldFindNoGroupViewWhenGroupNameIsMissing() throws Throwable
    {
        // Given
        Config config = new Config( stringMap(
                "0.user.name", "Bob",
                "0.user.age", "81",
                "1.user.name", "Greta",
                "1.user.age", "82" ) );

        Setting<String> name = setting( "user.name", STRING, NO_DEFAULT );
        Setting<Integer> age = setting( "user.age", INTEGER, NO_DEFAULT );

        // When
        List<Configuration> emptyStrViews = config.view( ConfigGroups.groups( "" ) );
        List<Configuration> numViews = config.view( ConfigGroups.groups( "0" ) );

        // Then
        assertThat( emptyStrViews.size(), equalTo( 0 ) );
        assertThat( numViews.size(), equalTo( 0 ) );
        assertThat( config.get( setting( "0.user.name", STRING, NO_DEFAULT ) ), equalTo( "Bob" ) );
    }

    @Test
    public void shouldFindNoGroupViewWhenGroupNameIsWrong() throws Throwable
    {
        // Given
        Config config = new Config( stringMap(
                "my.users.0.name", "Bob",
                "my.users.0.age", "81",
                "my.users.1.name", "Greta",
                "my.users.1.age", "82" ) );

        // When
        List<Configuration> views = config.view( ConfigGroups.groups( "my" ) );

        // Then
        assertThat( views.size(), equalTo( 0 ) );
    }

    @Test
    public void shouldOnlyReadInsideGroupWhileAccessingSettingsInAGroup() throws Throwable
    {
        // Given
        Config config = new Config( stringMap(
                "name", "lemon",
                "my.users.0.user.name", "Bob",
                "my.users.0.user.age", "81",
                "my.users.1.user.name", "Greta",
                "my.users.1.user.age", "82" ) );

        Setting<String> name = setting( "name", STRING, "No name given to this poor user" );
        Setting<Integer> age = setting( "age", INTEGER, NO_DEFAULT );

        // When
        List<Configuration> views = config.view( ConfigGroups.groups( "my.users" ) );

        // Then
        assertThat( views.size(), equalTo( 2 ) );

        Configuration bob = views.get( 0 );
        assertThat( bob.get( name ), equalTo( "No name given to this poor user" ) );
        assertNull( bob.get( age ) );

        Configuration greta = views.get( 1 );
        assertThat( greta.get( name ), equalTo( "No name given to this poor user" ) );
        assertNull( greta.get( age ) );

        assertThat( config.get( name ), equalTo( "lemon" ) );
        assertNull( config.get( age ) );
    }
}
