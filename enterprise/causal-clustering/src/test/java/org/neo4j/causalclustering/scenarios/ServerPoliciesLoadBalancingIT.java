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
package org.neo4j.causalclustering.scenarios;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.load_balancing.LoadBalancingResult;
import org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.Policies;
import org.neo4j.causalclustering.routing.load_balancing.procedure.ParameterNames;
import org.neo4j.causalclustering.routing.load_balancing.procedure.ResultFormatV1;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.routing.load_balancing.procedure.ProcedureNames.GET_SERVERS_V2;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ServerPoliciesLoadBalancingIT
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    private Cluster cluster;

    @After
    public void after()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void defaultBehaviour() throws Exception
    {
        cluster = new Cluster( testDir.directory( "cluster" ), 3, 3, new HazelcastDiscoveryServiceFactory(), emptyMap(),
                emptyMap(), emptyMap(), emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );

        cluster.start();

        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 2, 3 ) );
    }

    @Test
    public void defaultBehaviourWithAllowReadsOnFollowers() throws Exception
    {
        cluster = new Cluster( testDir.directory( "cluster" ), 3, 3,
                new HazelcastDiscoveryServiceFactory(),
                stringMap( CausalClusteringSettings.cluster_allow_reads_on_followers.name(), "true" ),
                emptyMap(), emptyMap(), emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );

        cluster.start();

        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 2, 3 ) );
    }

    @Test
    public void shouldFallOverBetweenRules() throws Exception
    {
        Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
        instanceCoreParams.put( CausalClusteringSettings.server_groups.name(), id -> "core" + id + ",core" );
        Map<String,IntFunction<String>> instanceReplicaParams = new HashMap<>();
        instanceReplicaParams.put( CausalClusteringSettings.server_groups.name(), id -> "replica" + id + ",replica" );

        String defaultPolicy = "groups(core) -> min(3); groups(replica1,replica2) -> min(2);";

        Map<String,String> coreParams = stringMap(
                CausalClusteringSettings.cluster_allow_reads_on_followers.name(), "true",
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.default", defaultPolicy,
                CausalClusteringSettings.multi_dc_license.name(), "true");

        cluster = new Cluster( testDir.directory( "cluster" ), 5, 5,
                new HazelcastDiscoveryServiceFactory(), coreParams, instanceCoreParams,
                emptyMap(), instanceReplicaParams, Standard.LATEST_NAME, IpFamily.IPV4, false );

        cluster.start();
        // should use the first rule: only cores for reading
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 5, 1, 4, 0 ) );

        cluster.getCoreMemberById( 3 ).shutdown();
        // one core reader is gone, but we are still fulfilling min(3)
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 4, 1, 3, 0 ) );

        cluster.getCoreMemberById( 0 ).shutdown();
        // should now fall over to the second rule: use replica1 and replica2
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 0, 2 ) );

        cluster.getReadReplicaById( 0 ).shutdown();
        // this does not affect replica1 and replica2
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 0, 2 ) );

        cluster.getReadReplicaById( 1 ).shutdown();
        // should now fall over to use the last rule: all
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 2, 3 ) );

        cluster.addCoreMemberWithId( 3 ).start();
        // should now go back to first rule
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 4, 1, 3, 0 ) );
    }

    @Test
    public void shouldSupportSeveralPolicies() throws Exception
    {
        Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
        instanceCoreParams.put( CausalClusteringSettings.server_groups.name(), id -> "core" + id + ",core" );
        Map<String,IntFunction<String>> instanceReplicaParams = new HashMap<>();
        instanceReplicaParams.put( CausalClusteringSettings.server_groups.name(), id -> "replica" + id + ",replica" );

        String defaultPolicySpec = "groups(replica0,replica1)";
        String policyOneTwoSpec = "groups(replica1,replica2)";
        String policyZeroTwoSpec = "groups(replica0,replica2)";
        String policyAllReplicasSpec = "groups(replica); halt()";
        String allPolicySpec = "all()";

        Map<String,String> coreParams = stringMap(
                CausalClusteringSettings.cluster_allow_reads_on_followers.name(), "true",
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.all", allPolicySpec,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.default", defaultPolicySpec,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.policy_one_two", policyOneTwoSpec,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.policy_zero_two", policyZeroTwoSpec,
                CausalClusteringSettings.load_balancing_config.name() + ".server_policies.policy_all_replicas", policyAllReplicasSpec,
                CausalClusteringSettings.multi_dc_license.name(), "true"
        );

        cluster = new Cluster( testDir.directory( "cluster" ), 3, 3,
                new HazelcastDiscoveryServiceFactory(), coreParams, instanceCoreParams,
                emptyMap(), instanceReplicaParams, Standard.LATEST_NAME, IpFamily.IPV4, false );

        cluster.start();
        assertGetServersEventuallyMatchesOnAllCores( new CountsMatcher( 3, 1, 2, 3 ), policyContext( "all" ) );
        // all cores have observed the full topology, now specific policies should all return the same result

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            CoreGraphDatabase db = core.database();

            assertThat( getServers( db, policyContext( "default" ) ), new SpecificReplicasMatcher( 0, 1 ) );
            assertThat( getServers( db, policyContext( "policy_one_two" ) ), new SpecificReplicasMatcher( 1, 2 ) );
            assertThat( getServers( db, policyContext( "policy_zero_two" ) ), new SpecificReplicasMatcher( 0, 2 ) );
            assertThat( getServers( db, policyContext( "policy_all_replicas" ) ), new SpecificReplicasMatcher( 0, 1, 2 ) );
        }
    }

    private Map<String,String> policyContext( String policyName )
    {
        return stringMap( Policies.POLICY_KEY, policyName );
    }

    private void assertGetServersEventuallyMatchesOnAllCores( Matcher<LoadBalancingResult> matcher ) throws InterruptedException
    {
        assertGetServersEventuallyMatchesOnAllCores( matcher, emptyMap() );
    }

    private void assertGetServersEventuallyMatchesOnAllCores( Matcher<LoadBalancingResult> matcher,
            Map<String,String> context ) throws InterruptedException
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            if ( core.database() == null )
            {
                // this core is shutdown
                continue;
            }

            assertEventually( matcher, () -> getServers( core.database(), context ) );
        }
    }

    private LoadBalancingResult getServers( CoreGraphDatabase db, Map<String,String> context )
    {
        LoadBalancingResult lbResult = null;
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.explicit, EnterpriseLoginContext.AUTH_DISABLED ) )
        {
            Map<String,Object> parameters = MapUtil.map( ParameterNames.CONTEXT.parameterName(), context );
            try ( Result result = db.execute( tx, "CALL " + GET_SERVERS_V2.callName(), ValueUtils.asMapValue( parameters )) )
            {
                while ( result.hasNext() )
                {
                    lbResult = ResultFormatV1.parse( result.next() );
                }
            }
        }
        return lbResult;
    }

    private static <T, E extends Exception> void assertEventually( Matcher<? super T> matcher,
            ThrowingSupplier<T,E> actual ) throws InterruptedException, E
    {
        org.neo4j.test.assertion.Assert.assertEventually( "", actual, matcher, 120, SECONDS );
    }

    class CountsMatcher extends BaseMatcher<LoadBalancingResult>
    {
        private final int nRouters;
        private final int nWriters;
        private final int nCoreReaders;
        private final int nReplicaReaders;

        CountsMatcher( int nRouters, int nWriters, int nCoreReaders, int nReplicaReaders )
        {
            this.nRouters = nRouters;
            this.nWriters = nWriters;
            this.nCoreReaders = nCoreReaders;
            this.nReplicaReaders = nReplicaReaders;
        }

        @Override
        public boolean matches( Object item )
        {
            LoadBalancingResult result = (LoadBalancingResult) item;

            if ( result.routeEndpoints().size() != nRouters ||
                 result.writeEndpoints().size() != nWriters )
            {
                return false;
            }

            Set<AdvertisedSocketAddress> allCoreBolts = cluster.coreMembers().stream()
                    .map( c -> c.clientConnectorAddresses().boltAddress() )
                    .collect( Collectors.toSet() );

            Set<AdvertisedSocketAddress> returnedCoreReaders = result.readEndpoints().stream()
                    .map( Endpoint::address )
                    .filter( allCoreBolts::contains )
                    .collect( Collectors.toSet() );

            if ( returnedCoreReaders.size() != nCoreReaders )
            {
                return false;
            }

            Set<AdvertisedSocketAddress> allReplicaBolts = cluster.readReplicas().stream()
                    .map( c -> c.clientConnectorAddresses().boltAddress() )
                    .collect( Collectors.toSet() );

            Set<AdvertisedSocketAddress> returnedReplicaReaders = result.readEndpoints().stream()
                    .map( Endpoint::address )
                    .filter( allReplicaBolts::contains )
                    .collect( Collectors.toSet() );

            if ( returnedReplicaReaders.size() != nReplicaReaders )
            {
                return false;
            }

            HashSet<AdvertisedSocketAddress> overlap = new HashSet<>( returnedCoreReaders );
            overlap.retainAll( returnedReplicaReaders );

            if ( !overlap.isEmpty() )
            {
                return false;
            }

            Set<AdvertisedSocketAddress> returnedWriters = result.writeEndpoints().stream()
                    .map( Endpoint::address )
                    .collect( Collectors.toSet() );

            if ( !allCoreBolts.containsAll( returnedWriters ) )
            {
                return false;
            }

            Set<AdvertisedSocketAddress> returnedRouters = result.routeEndpoints().stream()
                    .map( Endpoint::address )
                    .collect( Collectors.toSet() );

            //noinspection RedundantIfStatement
            if ( !allCoreBolts.containsAll( returnedRouters ) )
            {
                return false;
            }

            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "nRouters=" + nRouters );
            description.appendText( ", nWriters=" + nWriters );
            description.appendText( ", nCoreReaders=" + nCoreReaders );
            description.appendText( ", nReplicaReaders=" + nReplicaReaders );
        }
    }

    class SpecificReplicasMatcher extends BaseMatcher<LoadBalancingResult>
    {
        private final Set<Integer> replicaIds;

        SpecificReplicasMatcher( Integer... replicaIds )
        {
            this.replicaIds = Arrays.stream( replicaIds ).collect( Collectors.toSet() );
        }

        @Override
        public boolean matches( Object item )
        {
            LoadBalancingResult result = (LoadBalancingResult) item;

            Set<AdvertisedSocketAddress> returnedReaders = result.readEndpoints().stream()
                    .map( Endpoint::address )
                    .collect( Collectors.toSet() );

            Set<AdvertisedSocketAddress> expectedBolts = cluster.readReplicas().stream()
                    .filter( r -> replicaIds.contains( r.serverId() ) )
                    .map( r -> r.clientConnectorAddresses().boltAddress() )
                    .collect( Collectors.toSet() );

            return expectedBolts.equals( returnedReaders );
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "replicaIds=" + replicaIds );
        }
    }
}
