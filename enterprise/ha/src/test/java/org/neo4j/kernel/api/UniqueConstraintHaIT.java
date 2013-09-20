/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.InvalidTransactionTypeException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.UniquenessConstraintDefinition;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

public class UniqueConstraintHaIT
{

    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass(), clusterOfSize( 3 ) );

    @Test
    public void shouldCreateUniqueConstraintOnMaster() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();

        // when
        try ( Transaction tx = master.beginTx() )
        {
            master.schema().constraintFor( label( "Label1" ) ).on( "key1" ).unique().create();
            tx.success();
        }

        cluster.sync();

        // then
        for ( HighlyAvailableGraphDatabase clusterMember : cluster.getAllMembers() )
        {
            try ( Transaction tx = clusterMember.beginTx() )
            {
                UniquenessConstraintDefinition constraint =
                        single( clusterMember.schema().getConstraints( label( "Label1" ) ) )
                        .asUniquenessConstraint();
                assertEquals( "key1", single( constraint.getPropertyKeys() ) );
                tx.success();
            }
        }
    }

    @Test
    public void shouldNotBePossibleToCreateConstraintsDirectlyOnSlaves() throws Exception
    {
        // given
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        // when
        try ( Transaction tx = slave.beginTx() )
        {
            slave.schema().constraintFor( label( "Label1" ) ).on( "key1" ).unique().create();
            fail( "We expected to not be able to create a constraint on a slave in a cluster." );
        }
        catch ( Exception e )
        {
            assertThat(e, instanceOf(InvalidTransactionTypeException.class));
        }
    }
}
