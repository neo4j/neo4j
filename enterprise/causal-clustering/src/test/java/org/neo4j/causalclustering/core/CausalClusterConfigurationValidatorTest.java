/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.Mode;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;
import static org.neo4j.helpers.collection.MapUtil.stringMap;


@RunWith( Parameterized.class )
public class CausalClusterConfigurationValidatorTest
{
    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Parameterized.Parameter
    public Mode mode;

    @Parameterized.Parameters( name = "{0}" )
    public static List<Mode> recordFormats()
    {
        return Arrays.asList( Mode.CORE, Mode.READ_REPLICA );
    }

    @Test
    public void validateOnlyIfModeIsCoreOrReplica()
    {
        // when
        Config config = Config.fromSettings(
                stringMap( mode.name(), Mode.SINGLE.name(),
                        initial_discovery_members.name(), "" ) )
                .withValidator( new CausalClusterConfigurationValidator() ).build();

        // then
        Optional<String> value = config.getRaw( initial_discovery_members.name() );
        assertTrue( value.isPresent() );
        assertEquals( "", value.get() );
    }

    @Test
    public void validateSuccess()
    {
        // when
        Config config = Config.fromSettings(
                stringMap( mode.name(), mode.name(),
                        initial_discovery_members.name(), "localhost:99,remotehost:2",
                        new BoltConnector( "bolt" ).enabled.name(), "true" ))
                .withValidator( new CausalClusterConfigurationValidator() ).build();

        // then
        assertEquals( asList( new AdvertisedSocketAddress( "localhost", 99 ),
                new AdvertisedSocketAddress( "remotehost", 2 ) ),
                config.get( initial_discovery_members ) );
    }

    @Test
    public void missingInitialMembers()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "Missing mandatory non-empty value for 'causal_clustering.initial_discovery_members'" );

        // when
        Config.builder().withSetting( EnterpriseEditionSettings.mode, mode.name() ).withValidator( new CausalClusterConfigurationValidator() ).build();
    }

    @Test
    public void missingBoltConnector()
    {
        // then
        expected.expect( InvalidSettingException.class );
        expected.expectMessage( "A Bolt connector must be configured to run a cluster" );

        // when
        Config.fromSettings(
                stringMap( EnterpriseEditionSettings.mode.name(), mode.name(),
                        initial_discovery_members.name(), "localhost:99,remotehost:2" ) )
                .withValidator( new CausalClusterConfigurationValidator() ).build();
    }
}
