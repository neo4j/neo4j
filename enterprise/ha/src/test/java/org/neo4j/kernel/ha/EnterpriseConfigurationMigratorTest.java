/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import java.util.Map;

import org.junit.Test;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.logging.NullLog;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.kernel.ha.HaSettings.tx_push_strategy;

/**
 * EnterpriseConfigurationMigrator tests
 */
public class EnterpriseConfigurationMigratorTest
{
    EnterpriseConfigurationMigrator migrator = new EnterpriseConfigurationMigrator();
    
    @Test
    public void testFixedPushStrategyMigration()
            throws Exception
    {
        // Given
        Map<String, String> original = MapUtil.stringMap( tx_push_strategy.name(), "fixed" );

        // When
        Map<String, String> migrated = migrator.apply( original, NullLog.getInstance() );

        // Then
        assertThat( migrated.get( tx_push_strategy.name() ), equalTo( HaSettings.TxPushStrategy.fixed_descending.name() ) );
    }
}
