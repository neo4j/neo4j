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
        Config config = Config.defaults();

        assertFalse( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }

    @Test
    public void shouldReturnFalseIfLicenseIsUnsetAndRefuseToBeLeaderIsTrue() throws Exception
    {
        Config config = Config.defaults( CausalClusteringSettings.refuse_to_be_leader, "true" );

        assertFalse( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }

    @Test
    public void shouldLogWarningIfLicenseIsUnsetAndRefuseToBeLeaderIsTrue() throws Exception
    {
        Config config = Config.defaults( CausalClusteringSettings.refuse_to_be_leader, "true" );

        Log logMock = mock( Log.class );

        RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config, logMock );

        verify( logMock, times( 1 ) ).warn( anyString() );
    }

    @Test
    public void shouldReturnFalseIfLicenseIsFalseAndRefuseToBeLeaderIsTrue() throws Exception
    {
        Config config = Config.builder()
                .withSetting( CausalClusteringSettings.refuse_to_be_leader, "true" )
                .withSetting( CausalClusteringSettings.multi_dc_license, "false" ).build();

        assertFalse( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }

    @Test
    public void shouldReturnFalseIfLicenseIsFalseAndRefuseToBeLeaderIsFalse() throws Exception
    {
        Config config = Config.builder()
                .withSetting( CausalClusteringSettings.refuse_to_be_leader, "false" )
                .withSetting( CausalClusteringSettings.multi_dc_license, "false" ).build();

        assertFalse( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }

    @Test
    public void shouldReturnTrueIfLicenseIsTrueAndRefuseToBeLeaderIsTrue() throws Exception
    {
        Config config = Config.builder()
                .withSetting( CausalClusteringSettings.refuse_to_be_leader, "true" )
                .withSetting( CausalClusteringSettings.multi_dc_license, "true" ).build();

        assertTrue( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }

    @Test
    public void shouldReturnFalseIfLicenseIsTrueAndRefuseToBeLeaderIsUnset() throws Exception
    {
        Config config = Config.defaults( CausalClusteringSettings.multi_dc_license, "true" );

        assertFalse( RefuseToBeLeaderStrategy.shouldRefuseToBeLeader( config ) );
    }
}
