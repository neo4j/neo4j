/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Settings;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.Settings.BOOLEAN;
import static org.neo4j.helpers.Settings.STRING;
import static org.neo4j.helpers.Settings.setting;

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

        public static Setting<String> newer = setting( "hello", STRING, "" );
    }

    public static class MySettingsWithDefaults
    {
        public static Setting<String> hello = setting( "hello", STRING, "Hello, World!" );

        public static Setting<Boolean> boolSetting = setting( "bool_setting", BOOLEAN, Settings.TRUE );

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

        Map<String, String> params = new HashMap<String, String>();
        params.put( "old", "hello!" );

        Config config = new Config( params, MyMigratingSettings.class );
        assertThat( config.get( MyMigratingSettings.newer ), is( "hello!" ) );
    }

    @Test
    public void shouldNotAllowSettingInvalidValues()
    {
        Config config = new Config( new HashMap<String, String>(), MySettingsWithDefaults.class );

        try
        {

            Map<String, String> params = config.getParams();
            params.put( MySettingsWithDefaults.boolSetting.name(), "asd" );

            config.applyChanges( params );

            fail( "Expected validation to fail." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

    @Test
    public void shouldNotAllowInvalidValuesInConstructor()
    {
        try
        {
            new Config( new HashMap<String, String>()
            {{
                    put( MySettingsWithDefaults.boolSetting.name(), "asd" );
                }},
                    MySettingsWithDefaults.class );

            fail( "Expected validation to fail." );
        }
        catch ( IllegalArgumentException e )
        {
        }
    }

}
