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
package org.neo4j.causalclustering.load_balancing.strategy.server_policy;

import org.junit.Test;

import java.util.Set;

import org.neo4j.causalclustering.load_balancing.filters.Filter;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class PoliciesTest
{
    @Test
    public void shouldSupplyDefaultUnfilteredPolicy() throws Exception
    {
        // given
        Policies policies = new Policies( NullLogProvider.getInstance() );

        // when
        Filter<ServerInfo> filter = policies.selectFor( emptyMap() );
        Set<ServerInfo> input = asSet(
                new ServerInfo( new AdvertisedSocketAddress( "bolt", 1 ), asSet( "tagA" ) ),
                new ServerInfo( new AdvertisedSocketAddress( "bolt", 2 ), asSet( "tagB" ) )
        );

        Set<ServerInfo> output = filter.apply( input );

        // then
        assertEquals( input, output );
    }

    @Test
    public void shouldAllowLookupOfAddedPolicy() throws Exception
    {
        // given
        Policies policies = new Policies( NullLogProvider.getInstance() );

        String myPolicyName = "china";
        Filter<ServerInfo> myPolicy = data -> data;

        // when
        policies.addPolicy( myPolicyName, myPolicy );
        Filter<ServerInfo> selectedPolicy = policies.selectFor( stringMap( Policies.POLICY_KEY, myPolicyName ) );

        // then
        assertEquals( myPolicy, selectedPolicy );
    }
}
