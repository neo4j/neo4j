/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.ha;

import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cache_type;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;
import static org.neo4j.test.TargetDirectory.forTest;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

public class HaCacheIT
{
    private static final int DENSE_NODE = 10;
    public final @Rule TestDirectory root = forTest( getClass() ).testDirectory();

    @Test
    public void shouldUpdateSlaveCacheWhenRemovingRelationshipGroupFromDenseNode() throws Throwable
    {
        ClusterManager manager = new ClusterManager( clusterOfSize( 3 ), root.directory(),
                                                     stringMap( tx_push_factor.name(), "2",
                                                                cache_type.name(), "strong",
                                                                dense_node_threshold.name(), "" + DENSE_NODE ) );
        try
        {
            // given
            manager.start();
            ClusterManager.ManagedCluster cluster = manager.getDefaultCluster();
            cluster.await( ClusterManager.masterAvailable() );
            cluster.await( ClusterManager.masterSeesAllSlavesAsAvailable() );
            HighlyAvailableGraphDatabase master = cluster.getMaster();
            long nodeId; // a dense node
            try ( Transaction tx = master.beginTx() )
            {
                Node node = master.createNode();
                for ( int i = 0; i < DENSE_NODE; i++ )
                {
                    node.createRelationshipTo( master.createNode(), withName( "FOO" ) );
                }
                master.createNode().createRelationshipTo( node, withName( "BAR" ) );

                tx.success();
                nodeId = node.getId();
            }
            // fully cache node on all instances
            int count = 0;
            for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
            {
                try ( Transaction tx = db.beginTx() )
                {
                    int these = count( db.getNodeById( nodeId ).getRelationships() );
                    assertTrue( String.format( "expected=%s, count here=%s", count, these ),
                                these != 0 && (count == 0 || these == count) );
                    count = these;
                    tx.success();
                }
            }

            // when
            try ( Transaction tx = master.beginTx() )
            {
                for ( Relationship relationship : master.getNodeById( nodeId ).getRelationships( withName( "BAR" ) ) )
                {
                    relationship.delete();
                }
                tx.success();
            }

            // then
            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
            try ( Transaction tx = slave.beginTx() )
            {
                List<String> relationships = new ArrayList<>();
                for ( Relationship relationship : slave.getNodeById( nodeId ).getRelationships() )
                {
                    relationships.add( String.format( "(%d)-[%d:%s]->(%d)",
                                                      relationship.getStartNode().getId(),
                                                      relationship.getId(), relationship.getType().name(),
                                                      relationship.getEndNode().getId() ) );
                }
                assertEquals( joinLines( relationships ), count - 1, relationships.size() );
                assertEquals( count - 1, count( slave.getNodeById( nodeId ).getRelationships() ) );

                tx.success();
            }
        }
        finally
        {
            manager.shutdown();
        }
    }

    private String joinLines( Iterable<String> lines )
    {
        StringBuilder result = new StringBuilder();
        for ( String line : lines )
        {
            result.append( "\n\t" ).append( line );
        }
        return result.toString();
    }
}
