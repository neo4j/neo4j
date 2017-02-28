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
package org.neo4j.causalclustering.scenarios;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ServerTagsIT
{
    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    private Cluster cluster;

    @After
    public void after() throws Exception
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldUpdateTagsOnStart() throws Exception
    {
        AtomicReference<String> suffix = new AtomicReference<>( "before" );
        List<List<String>> expected;

        Map<String,IntFunction<String>> instanceCoreParams = new HashMap<>();
        instanceCoreParams.put( CausalClusteringSettings.server_tags.name(), ( id ) -> String.join( ", ", makeCoreTags( suffix.get(), id ) ) );

        Map<String,IntFunction<String>> instanceReplicaParams = new HashMap<>();
        instanceReplicaParams.put( CausalClusteringSettings.server_tags.name(), ( id ) -> String.join( ", ", makeReplicaTags( suffix.get(), id ) ) );

        int nServers = 3;
        cluster = new Cluster( testDir.directory( "cluster" ), nServers, nServers,
                new HazelcastDiscoveryServiceFactory(), emptyMap(), instanceCoreParams,
                emptyMap(), instanceReplicaParams, Standard.LATEST_NAME );

        // when
        cluster.start();

        // then
        expected = new ArrayList<>();
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            expected.add( makeCoreTags( suffix.get(), core.serverId() ) );
            expected.add( makeReplicaTags( suffix.get(), core.serverId() ) );
        }

        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            assertEventually( core + " should have tags", () -> getServerTags( core.database() ),
                    new TagsMatcher( expected ), 30, SECONDS );
        }

        // when
        expected.remove( makeCoreTags( suffix.get(), 1 ) );
        expected.remove( makeReplicaTags( suffix.get(), 2 ) );
        cluster.getCoreMemberById( 1 ).shutdown();
        cluster.getReadReplicaById( 2 ).shutdown();

        suffix.set( "after" ); // should update tags of restarted servers
        cluster.addCoreMemberWithId( 1 ).start();
        cluster.addReadReplicaWithId( 2 ).start();
        expected.add( makeCoreTags( suffix.get(), 1 ) );
        expected.add( makeReplicaTags( suffix.get(), 2 ) );

        // then
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            assertEventually( core + " should have tags", () -> getServerTags( core.database() ),
                    new TagsMatcher( expected ), 30, SECONDS );
        }
    }

    class TagsMatcher extends TypeSafeMatcher<List<List<String>>>
    {
        private final List<List<String>> expected;

        TagsMatcher( List<List<String>> expected )
        {
            this.expected = expected;
        }

        @Override
        protected boolean matchesSafely( List<List<String>> actual )
        {
            if ( actual.size() != expected.size() )
            {
                return false;
            }

            for ( List<String> actualTags : actual )
            {
                boolean matched = false;
                for ( List<String> expectedTags : expected )
                {
                    if ( actualTags.size() != expectedTags.size() )
                    {
                        continue;
                    }

                    if ( !actualTags.containsAll( expectedTags ) )
                    {
                        continue;
                    }

                    matched = true;
                    break;
                }

                if ( !matched )
                {
                    return false;
                }
            }

            return true;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( expected.toString() );
        }
    }

    private List<String> makeCoreTags( String suffix, int id )
    {
        return asList( format( "core-%d-%s", id, suffix ), "core" );
    }

    private List<String> makeReplicaTags( String suffix, int id )
    {
        return asList( format( "replica-%d-%s", id, suffix ), "replica" );
    }

    private List<List<String>> getServerTags( CoreGraphDatabase db )
    {
        List<List<String>> serverTags = new ArrayList<>();
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.explicit, EnterpriseSecurityContext.AUTH_DISABLED ) )
        {
            try ( Result result = db.execute( tx, "CALL dbms.cluster.overview", emptyMap() ) )
            {
                while ( result.hasNext() )
                {
                    @SuppressWarnings( "unchecked" )
                    List<String> tags = (List<String>) result.next().get( "tags" );
                    serverTags.add( tags );
                }
            }
        }
        return serverTags;
    }
}
