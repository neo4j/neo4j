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
package org.neo4j.kernel.impl.storemigration;

import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;

import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.storemigration.MigrationTestUtils.defaultConfig;

public class ConfigMapUpgradeConfigurationTestIT
{
    @Test
    public void shouldNotAllowAutomaticUpgradeIfConfigParameterIsMissing()
    {
        Config config = defaultConfig();
        assertFalse( config.get( GraphDatabaseSettings.allow_store_upgrade ) );

        try
        {
            new ConfigMapUpgradeConfiguration( config ).checkConfigurationAllowsAutomaticUpgrade();
            fail( "Should throw exception" );
        } catch ( UpgradeNotAllowedByConfigurationException e )
        {
            // expected
        }
    }

    @Test
    public void shouldNotAllowAutomaticUpgradeIfConfigParameterIsFalse()
    {
        Config config = defaultConfig( stringMap( GraphDatabaseSettings.allow_store_upgrade.name(), "false" ) );

        try
        {
            new ConfigMapUpgradeConfiguration( config ).checkConfigurationAllowsAutomaticUpgrade();
            fail( "Should throw exception" );
        } catch ( UpgradeNotAllowedByConfigurationException e )
        {
            // expected
        }
    }

    @Test
    public void shouldNotAllowAutomaticUpgradeIfConfigParameterIsTrue()
    {
        Config config = defaultConfig( stringMap( GraphDatabaseSettings.allow_store_upgrade.name(), "false" ) );

        try
        {
            new ConfigMapUpgradeConfiguration( config ).checkConfigurationAllowsAutomaticUpgrade();
            fail( "Should throw exception" );
        } catch ( UpgradeNotAllowedByConfigurationException e )
        {
            // expected
        }
    }
}
