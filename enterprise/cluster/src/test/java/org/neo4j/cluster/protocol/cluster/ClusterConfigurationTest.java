/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cluster.protocol.cluster;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.test.IterableMatcher.matchesIterable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;
import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.NullLogProvider;

public class ClusterConfigurationTest
{
    public static URI NEO4J_SERVER1_URI;
    public static InstanceId NEO4J_SERVER_ID;

    static
    {
        try
        {
            NEO4J_SERVER1_URI = new URI( "neo4j://server1" );
            NEO4J_SERVER_ID = new InstanceId( 1 );
        }
        catch ( URISyntaxException e )
        {
            e.printStackTrace();
        }
    }

    ClusterConfiguration configuration = new ClusterConfiguration( "default", NullLogProvider.getInstance(), new ArrayList<URI>() );

    @Test
    public void givenEmptyClusterWhenNodeAddedThenNodeWasAdded()
    {
        configuration.joined( NEO4J_SERVER_ID, NEO4J_SERVER1_URI );

        assertThat( configuration.getMemberIds(), matchesIterable( Iterables.<InstanceId, InstanceId>iterable( NEO4J_SERVER_ID ) ) );
        assertThat( configuration.getUriForId( NEO4J_SERVER_ID ), equalTo( NEO4J_SERVER1_URI ) );
        assertThat( configuration.getMemberURIs(), equalTo( Arrays.asList( NEO4J_SERVER1_URI ) ) );
    }

    @Test
    public void givenEmptyClusterWhenNodeIsAddedTwiceThenNodeWasAddedOnce()
    {
        configuration.joined( NEO4J_SERVER_ID, NEO4J_SERVER1_URI );
        configuration.joined( NEO4J_SERVER_ID, NEO4J_SERVER1_URI );

        assertThat( configuration.getMemberIds(), matchesIterable( Iterables.<InstanceId, InstanceId>iterable( NEO4J_SERVER_ID ) ) );
        assertThat( configuration.getUriForId( NEO4J_SERVER_ID ), equalTo( NEO4J_SERVER1_URI ) );
        assertThat( configuration.getMemberURIs(), equalTo( Arrays.asList( NEO4J_SERVER1_URI ) ) );
    }

    @Test
    public void givenClusterWithOneNodeWhenNodeIsRemovedThenClusterIsEmpty()
    {
        configuration.joined( NEO4J_SERVER_ID, NEO4J_SERVER1_URI );
        configuration.left( NEO4J_SERVER_ID );

        assertThat( configuration.getMemberIds(), matchesIterable( Iterables.<InstanceId>empty() ) );
        assertThat( configuration.getUriForId( NEO4J_SERVER_ID ), equalTo( null ) );
        assertThat( configuration.getMemberURIs(), equalTo( Collections.<URI>emptyList() ) );

    }

    @Test
    public void givenClusterWithOneNodeWhenNodeIsRemovedTwiceThenClusterIsEmpty()
    {
        configuration.joined( NEO4J_SERVER_ID, NEO4J_SERVER1_URI );
        configuration.left( NEO4J_SERVER_ID );
        configuration.left( NEO4J_SERVER_ID );

        assertThat( configuration.getMemberIds(), matchesIterable( Iterables.<InstanceId>empty() ) );
        assertThat( configuration.getUriForId( NEO4J_SERVER_ID ), equalTo( null ) );
        assertThat( configuration.getMemberURIs(), equalTo( Collections.<URI>emptyList() ) );

    }
}

