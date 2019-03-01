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
package org.neo4j.dbms.database;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.Settings.STRING;
import static org.neo4j.configuration.Settings.setting;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

class OverriddenDatabaseConfigTest
{

    @Test
    void shouldOnlyReflectAugmentsToUnderlyingConfigIfNotOverridden()
    {
        //given
        String expectedDbName = "foo";
        Config underlyingConfig = Config.defaults();
        OverriddenDatabaseConfig dbConfig = new OverriddenDatabaseConfig( underlyingConfig,
                stringMap( GraphDatabaseSettings.default_database.name(), expectedDbName ) );

        //when
        underlyingConfig.augment( GraphDatabaseSettings.allow_upgrade, "true" );
        underlyingConfig.augment( GraphDatabaseSettings.default_database, "bar" );

        //then
        assertTrue( dbConfig.get( GraphDatabaseSettings.allow_upgrade ),
                "Expected Overridden config to reflect underlying update to non overridden setting!" );
        assertEquals( expectedDbName, dbConfig.get( GraphDatabaseSettings.default_database ),
                "Expected Overridden config to ignore underlying updates to overridden setting!" );
    }

    @Test
    void shouldAugmentOverriddenValuesNotUnderlyingConfig()
    {
        //given
        Config underlyingConfig = Config.defaults();
        OverriddenDatabaseConfig dbConfig = new OverriddenDatabaseConfig( underlyingConfig,
                stringMap( GraphDatabaseSettings.default_database.name(), GraphDatabaseSettings.default_database.getDefaultValue() ) );

        //when
        dbConfig.augment( GraphDatabaseSettings.allow_upgrade, "true" );
        dbConfig.augment( GraphDatabaseSettings.default_database, "bar" );

        //then
        assertTrue( underlyingConfig.get( GraphDatabaseSettings.allow_upgrade ) &&
                dbConfig.get( GraphDatabaseSettings.allow_upgrade ) &&
                !Boolean.valueOf( GraphDatabaseSettings.allow_upgrade.getDefaultValue() ),
                "Both underlying and overridden config should reflect change to allow_upgrade!" );
        assertNotEquals( dbConfig.get( GraphDatabaseSettings.default_database ),
                underlyingConfig.get( GraphDatabaseSettings.default_database ),
                "Underlying and overridden config should diverge when an overridden setting is augmented!" );
        assertNotEquals( dbConfig.get( GraphDatabaseSettings.default_database ),
                GraphDatabaseSettings.default_database.getDefaultValue(),
                "Overridden config should diverge from default when augmented!" );
    }

    @Test
    void shouldCorrectlyReportIsConfiguredForOverriddenValues()
    {
        //given
        Setting<String> unConfiguredSetting = setting( "foo", STRING, "foo you too" );
        Config underlyingConfig = Config.defaults();
        OverriddenDatabaseConfig dbConfig = new OverriddenDatabaseConfig( underlyingConfig,
                stringMap( unConfiguredSetting.name(), "bar, actually" ) );

        //then
        assertTrue( dbConfig.isConfigured( unConfiguredSetting ), "Expected the setting to be configured in the overridden config!" );
        assertFalse( underlyingConfig.isConfigured( unConfiguredSetting ), "Did not expect the setting to be configured in the underlying config!" );
    }
}
