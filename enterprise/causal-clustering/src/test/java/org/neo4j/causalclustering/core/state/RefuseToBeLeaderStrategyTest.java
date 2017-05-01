/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state;

import org.junit.Test;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RefuseToBeLeaderStrategyTest
{
    @Test
    public void shouldReturnFalseByDefault() throws Exception
    {
        Config config = Config.empty();

        assertFalse( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }

    @Test
    public void shouldReturnFalseIfLicenseIsUnsetAndRefuseToBeLeaderIsTrue() throws Exception
    {
        Config config = Config.empty();
        config.augment( MapUtil.stringMap( CausalClusteringSettings.refuse_to_be_leader.name(), "true" ) );

        assertFalse( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }

    @Test
    public void shouldLogWarningIfLicenseIsUnsetAndRefuseToBeLeaderIsTrue() throws Exception
    {
        Config config = Config.empty();
        config.augment( MapUtil.stringMap( CausalClusteringSettings.refuse_to_be_leader.name(), "true" ) );

        Log logMock = mock( Log.class );

        RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config, logMock );

        verify( logMock, times( 1 ) ).warn( anyString() );
    }

    @Test
    public void shouldReturnFalseIfLicenseIsFalseAndRefuseToBeLeaderIsTrue() throws Exception
    {
        Config config = Config.empty();
        config.augment( MapUtil.stringMap( CausalClusteringSettings.refuse_to_be_leader.name(), "true" ) );
        config.augment( MapUtil.stringMap( CausalClusteringSettings.multi_dc_license.name(), "false" ) );

        assertFalse( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }

    @Test
    public void shouldReturnFalseIfLicenseIsFalseAndRefuseToBeLeaderIsFalse() throws Exception
    {
        Config config = Config.empty();
        config.augment( MapUtil.stringMap( CausalClusteringSettings.refuse_to_be_leader.name(), "false" ) );
        config.augment( MapUtil.stringMap( CausalClusteringSettings.multi_dc_license.name(), "false" ) );

        assertFalse( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }

    @Test
    public void shouldReturnTrueIfLicenseIsTrueAndRefuseToBeLeaderIsTrue() throws Exception
    {
        Config config = Config.empty();
        config.augment( MapUtil.stringMap( CausalClusteringSettings.refuse_to_be_leader.name(), "true" ) );
        config.augment( MapUtil.stringMap( CausalClusteringSettings.multi_dc_license.name(), "true" ) );

        assertTrue( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }

    @Test
    public void shouldReturnFalseIfLicenseIsTrueAndRefuseToBeLeaderIsUnset() throws Exception
    {
        Config config = Config.empty();
        config.augment( MapUtil.stringMap( CausalClusteringSettings.multi_dc_license.name(), "true" ) );

        assertFalse( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }
}
