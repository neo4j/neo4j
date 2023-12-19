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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.routing.Endpoint;
import org.neo4j.causalclustering.routing.multi_cluster.MultiClusterRoutingResult;
import org.neo4j.causalclustering.routing.multi_cluster.procedure.MultiClusterRoutingResultFormat;
import org.neo4j.causalclustering.routing.multi_cluster.procedure.ProcedureNames;
import org.neo4j.graphdb.Result;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.routing.multi_cluster.procedure.ParameterNames.DATABASE;
import static org.neo4j.causalclustering.scenarios.DiscoveryServiceType.SHARED;
import static org.neo4j.causalclustering.scenarios.DiscoveryServiceType.HAZELCAST;
import static org.neo4j.causalclustering.routing.multi_cluster.procedure.ProcedureNames.GET_ROUTERS_FOR_DATABASE;
import static org.neo4j.causalclustering.routing.multi_cluster.procedure.ProcedureNames.GET_ROUTERS_FOR_ALL_DATABASES;
import static org.neo4j.test.assertion.Assert.assertEventually;

@RunWith( Parameterized.class )
public class MultiClusterRoutingIT
{

    private static Set<String> DB_NAMES_1 = Stream.of( "foo", "bar" ).collect( Collectors.toSet() );
    private static Set<String> DB_NAMES_2 = Collections.singleton( "default" );
    private static Set<String> DB_NAMES_3 = Stream.of( "foo", "bar", "baz" ).collect( Collectors.toSet() );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]
                {
                        { "[shared discovery, 6 core hosts, 2 databases]", 6, 0, DB_NAMES_1, SHARED },
                        { "[hazelcast discovery, 6 core hosts, 2 databases]", 6, 0, DB_NAMES_1, HAZELCAST },
                        { "[shared discovery, 5 core hosts, 1 database]", 5, 0, DB_NAMES_2, SHARED },
                        { "[hazelcast discovery, 5 core hosts, 1 database]", 5, 0, DB_NAMES_2, HAZELCAST },
                        { "[hazelcast discovery, 6 core hosts, 3 read replicas, 3 databases]", 9, 3, DB_NAMES_3, HAZELCAST },
                        { "[shared discovery, 6 core hosts, 3 read replicas, 3 databases]", 8, 2, DB_NAMES_3, SHARED }
                }
        );
    }

    private final Set<String> dbNames;
    private final ClusterRule clusterRule;
    private final DefaultFileSystemRule fileSystemRule;
    private final DiscoveryServiceType discoveryType;
    private final int numCores;

    private Cluster cluster;
    private FileSystemAbstraction fs;

    @Rule
    public final RuleChain ruleChain;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(300);

    public MultiClusterRoutingIT( String ignoredName, int numCores, int numReplicas, Set<String> dbNames, DiscoveryServiceType discoveryType )
    {
        this.dbNames = dbNames;
        this.discoveryType = discoveryType;

        this.clusterRule = new ClusterRule()
                .withNumberOfCoreMembers( numCores )
                .withNumberOfReadReplicas( numReplicas )
                .withDatabaseNames( dbNames );
        this.numCores = numCores;

        this.fileSystemRule = new DefaultFileSystemRule();
        this.ruleChain = RuleChain.outerRule( fileSystemRule ).around( clusterRule );
    }

    @Before
    public void setup() throws Exception
    {
        clusterRule.withDiscoveryServiceType( discoveryType );
        fs = fileSystemRule.get();
        cluster = clusterRule.startCluster();
    }

    @Test
    public void superCallShouldReturnAllRouters()
    {
        List<CoreGraphDatabase> dbs = dbNames.stream()
                .map( n -> cluster.getMemberWithAnyRole( n, Role.FOLLOWER, Role.LEADER ).database() ).collect( Collectors.toList() );

        Stream<Optional<MultiClusterRoutingResult>> optResults = dbs.stream()
                .map( db -> callProcedure( db, GET_ROUTERS_FOR_ALL_DATABASES, Collections.emptyMap() ) );

        List<MultiClusterRoutingResult> results = optResults.filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() );
        assertEquals("There should be a result for each database against which the procedure is executed.",  dbNames.size(), results.size() );

        boolean consistentResults = results.stream().distinct().count() == 1;
        assertThat( "The results should be the same, regardless of which database the procedure is executed against.", consistentResults );

        Function<Map<String,List<Endpoint>>, Integer> countHosts = m -> m.values().stream().mapToInt( List::size ).sum();
        int resultsAllCores = results.stream().findFirst().map(r -> countHosts.apply( r.routers() ) ).orElse( 0 );
        assertEquals( "The results of the procedure should return all core hosts in the topology.", numCores, resultsAllCores );
    }

    @Test
    public void subCallShouldReturnLocalRouters()
    {
        String dbName = getFirstDbName( dbNames );
        Stream<CoreGraphDatabase> members = dbNames.stream().map( n -> cluster.getMemberWithAnyRole( n, Role.FOLLOWER, Role.LEADER ).database() );

        Map<String,Object> params = new HashMap<>();
        params.put( DATABASE.parameterName(), dbName );
        Stream<Optional<MultiClusterRoutingResult>> optResults = members.map( db -> callProcedure( db, GET_ROUTERS_FOR_DATABASE, params ) );
        List<MultiClusterRoutingResult> results = optResults.filter( Optional::isPresent ).map( Optional::get ).collect( Collectors.toList() );

        boolean consistentResults = results.stream().distinct().count() == 1;
        assertThat( "The results should be the same, regardless of which database the procedure is executed against.", consistentResults );

        Optional<MultiClusterRoutingResult> firstResult = results.stream().findFirst();

        int numRouterSets = firstResult.map( r -> r.routers().size() ).orElse( 0 );
        assertEquals( "There should only be routers returned for a single database.", 1, numRouterSets );

        boolean correctResultDbName = firstResult.map( r -> r.routers().containsKey( dbName ) ).orElse( false );
        assertThat( "The results should contain routers for the database passed to the procedure.", correctResultDbName );
    }

    @Test
    public void procedureCallsShouldReflectMembershipChanges() throws Exception
    {
        String dbName = getFirstDbName( dbNames );
        CoreClusterMember follower = cluster.getMemberWithAnyRole( dbName, Role.FOLLOWER );
        int followerId = follower.serverId();

        cluster.removeCoreMemberWithServerId( followerId );

        CoreGraphDatabase db = cluster.getMemberWithAnyRole( dbName, Role.FOLLOWER, Role.LEADER ).database();

        Function<CoreGraphDatabase, Set<Endpoint>> getResult = database ->
        {
            Optional<MultiClusterRoutingResult> optResult = callProcedure( database, GET_ROUTERS_FOR_ALL_DATABASES, Collections.emptyMap() );

            return optResult.map( r ->
                    r.routers().values().stream()
                            .flatMap( List::stream )
                            .collect( Collectors.toSet() )
            ).orElse( Collections.emptySet() );
        };

        assertEventually( "The procedure should return one fewer routers when a core member has been removed.",
                () -> getResult.apply( db ).size(), is(numCores - 1 ), 15, TimeUnit.SECONDS );

        BiPredicate<Set<Endpoint>, CoreClusterMember> containsFollower = ( rs, f ) ->
                rs.stream().anyMatch( r -> r.address().toString().equals( f.boltAdvertisedAddress() ) );

        assertEventually( "The procedure should not return a host as a router after it has been removed from the cluster",
                () -> containsFollower.test( getResult.apply( db ), follower ), is( false ), 15, TimeUnit.SECONDS );

        CoreClusterMember newFollower = cluster.addCoreMemberWithId( followerId );
        newFollower.start();

        assertEventually( "The procedure should return one more router when a core member has been added.",
                () -> getResult.apply( db ).size(), is( numCores ), 15, TimeUnit.SECONDS );
        assertEventually( "The procedure should return a core member as a router after it has been added to the cluster",
                () -> containsFollower.test( getResult.apply( db ), newFollower ), is( true ), 15, TimeUnit.SECONDS );

    }

    private static String getFirstDbName( Set<String> dbNames )
    {
        return dbNames.stream()
                .findFirst()
                .orElseThrow( () -> new IllegalArgumentException( "The dbNames parameter must not be empty." ) );
    }

    private static Optional<MultiClusterRoutingResult> callProcedure( CoreGraphDatabase db, ProcedureNames procedure, Map<String,Object> params )
    {

        Optional<MultiClusterRoutingResult> routingResult = Optional.empty();
        try (
                InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.explicit, EnterpriseLoginContext.AUTH_DISABLED );
                Result result = db.execute( tx, "CALL " + procedure.callName(), ValueUtils.asMapValue( params )) )
        {
            if ( result.hasNext() )
            {
                routingResult = Optional.of( MultiClusterRoutingResultFormat.parse( result.next() ) );
            }
        }
        return routingResult;
    }

}
