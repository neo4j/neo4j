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
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import org.junit.Test;

import java.util.Set;
import java.util.UUID;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class PoliciesTest
{
    private Log log = mock( Log.class );

    @Test
    public void shouldSupplyDefaultUnfilteredPolicyForEmptyContext() throws Exception
    {
        // given
        Policies policies = new Policies( log );

        // when
        Policy policy = policies.selectFor( emptyMap() );
        Set<ServerInfo> input = asSet(
                new ServerInfo( new AdvertisedSocketAddress( "bolt", 1 ), new MemberId( UUID.randomUUID() ), asSet( "groupA" ) ),
                new ServerInfo( new AdvertisedSocketAddress( "bolt", 2 ), new MemberId( UUID.randomUUID() ), asSet( "groupB" ) )
        );

        Set<ServerInfo> output = policy.apply( input );

        // then
        assertEquals( input, output );
        assertEquals( Policies.DEFAULT_POLICY, policy );
    }

    @Test
    public void shouldThrowExceptionOnUnknownPolicyName()
    {
        // given
        Policies policies = new Policies( log );

        try
        {
            // when
            policies.selectFor( stringMap( Policies.POLICY_KEY, "unknown-policy" ) );
            fail();
        }
        catch ( ProcedureException e )
        {
            // then
            assertEquals( Status.Procedure.ProcedureCallFailed, e.status() );
        }
    }

    @Test
    public void shouldThrowExceptionOnSelectionOfUnregisteredDefault()
    {
        Policies policies = new Policies( log );

        try
        {
            // when
            policies.selectFor( stringMap( Policies.POLICY_KEY, Policies.DEFAULT_POLICY_NAME ) );
            fail();
        }
        catch ( ProcedureException e )
        {
            // then
            assertEquals( Status.Procedure.ProcedureCallFailed, e.status() );
        }
    }

    @Test
    public void shouldAllowOverridingDefaultPolicy() throws Exception
    {
        Policies policies = new Policies( log );

        String defaulyPolicyName = Policies.DEFAULT_POLICY_NAME;
        Policy defaultPolicy = new FilteringPolicy( new AnyGroupFilter( "groupA", "groupB" ) );

        // when
        policies.addPolicy( defaulyPolicyName, defaultPolicy );
        Policy selectedPolicy = policies.selectFor( emptyMap() );

        // then
        assertEquals( defaultPolicy, selectedPolicy );
        assertNotEquals( Policies.DEFAULT_POLICY, selectedPolicy );
    }

    @Test
    public void shouldAllowLookupOfAddedPolicy() throws Exception
    {
        // given
        Policies policies = new Policies( log );

        String myPolicyName = "china";
        Policy myPolicy = data -> data;

        // when
        policies.addPolicy( myPolicyName, myPolicy );
        Policy selectedPolicy = policies.selectFor( stringMap( Policies.POLICY_KEY, myPolicyName ) );

        // then
        assertEquals( myPolicy, selectedPolicy );
    }
}
