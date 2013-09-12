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

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.UniquenessConstraintDefinition;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertEquals;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

public class UniqueConstraintHaIT
{
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

        // then
        try ( Transaction tx = master.beginTx() )
        {
            UniquenessConstraintDefinition constraint = single( master.schema().getConstraints( label( "Label1" ) ) )
                    .asUniquenessConstraint();
            assertEquals( "key1", single( constraint.getPropertyKeys() ) );
            tx.success();
        }
    }

    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass(), clusterOfSize( 3 ) );
}
