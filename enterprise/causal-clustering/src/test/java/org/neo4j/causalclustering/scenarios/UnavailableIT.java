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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class UnavailableIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() ).withNumberOfCoreMembers( 3 );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldReturnUnavailableStatus() throws Exception
    {
        // given
        ClusterMember member = cluster.getCoreMemberById( 1 );

        // when
        member.database().getDependencyResolver().resolveDependency( AvailabilityGuard.class )
                .require( () -> "Not doing long operation" );

        // then
        try ( Transaction tx = member.database().beginTx() )
        {
            tx.success();
            fail();
        }
        catch ( Exception e )
        {
            assertEquals( Status.General.DatabaseUnavailable, statusCodeOf( e ) );
        }
    }

    private Status statusCodeOf( Throwable e )
    {
        do
        {
            if ( e instanceof Status.HasStatus )
            {
                return ((Status.HasStatus) e).status();
            }
            e = e.getCause();
        }
        while ( e != null );

        return null;
    }
}
