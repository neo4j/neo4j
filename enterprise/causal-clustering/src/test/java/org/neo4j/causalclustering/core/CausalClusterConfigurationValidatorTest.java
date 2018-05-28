/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static org.neo4j.helpers.collection.MapUtil.stringMap;


@RunWith( Parameterized.class )
public class CausalClusterConfigurationValidatorTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Parameterized.Parameter
    public ClusterSettings.Mode mode;

    @Parameterized.Parameters( name = "{0}" )
    public static List<ClusterSettings.Mode> recordFormats()
    {
        return Arrays.asList( ClusterSettings.Mode.CORE, ClusterSettings.Mode.READ_REPLICA );
    }

    @Test
    public void validateOnlyIfModeIsCoreOrReplica() throws Exception
    {
        // when
        Config config = Config.embeddedDefaults(
                stringMap( ClusterSettings.mode.name(), ClusterSettings.Mode.SINGLE.name(),
                        initial_discovery_members.name(), "" ),
                Collections.singleton( new CausalClusterConfigurationValidator() ) );

        // then
        assertEquals( "", config.getRaw( initial_discovery_members.name() ).get() );
    }

    @Test
    public void validateSuccess() throws Exception
    {
        // when
        Config config = Config.embeddedDefaults(
                stringMap( ClusterSettings.mode.name(), mode.name(),
                        initial_discovery_members.name(), "localhost:99,remotehost:2",
                        new BoltConnector( "bolt" ).enabled.name(), "true" ),
                Collections.singleton( new CausalClusterConfigurationValidator() ) );

        // then
        assertEquals( asList( new AdvertisedSocketAddress( "localhost", 99 ),
                new AdvertisedSocketAddress( "remotehost", 2 ) ),
                config.get( initial_discovery_members ) );
    }

    @Test
    public void missingInitialMembers() throws Exception
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Missing mandatory non-empty value for 'causal_clustering.initial_discovery_members'" );

        // when
        Config.embeddedDefaults(
                stringMap( ClusterSettings.mode.name(), mode.name() ),
                Collections.singleton( new CausalClusterConfigurationValidator() ) );
    }

    @Test
    public void missingBoltConnector() throws Exception
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "A Bolt connector must be configured to run a cluster" );

        // when
        Config.embeddedDefaults(
                stringMap( ClusterSettings.mode.name(), mode.name(),
                        initial_discovery_members.name(), "localhost:99,remotehost:2" ),
                Collections.singleton( new CausalClusterConfigurationValidator() ) );
    }
}
