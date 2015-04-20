/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster.member;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;
import org.neo4j.tooling.GlobalGraphOperations;

public class HighAvailabilitySlavesIT
{
    private final TargetDirectory DIR = TargetDirectory.forTest( getClass() );
    private ClusterManager clusterManager;
    
    @After
    public void after() throws Throwable
    {
        clusterManager.stop();
    }
    
    @Test
    public void transactionsGetsPushedToSlaves() throws Throwable
    {
        // given
        clusterManager = new ClusterManager( clusterOfSize( 3 ), DIR.cleanDirectory( "dbs" ),
                stringMap( tx_push_factor.name(), "2" ) );
        clusterManager.start();
        ManagedCluster cluster = clusterManager.getDefaultCluster();

        // when
        String name = "a node";
        long node = createNode( cluster.getMaster(), name );

        // then
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() ) {
            Transaction transaction = db.beginTx();
            try
            {
                assertEquals( node, getNodeByName( db, name ) );
            }
            finally
            {
                transaction.finish();
            }
        }
    }

    private long getNodeByName( HighlyAvailableGraphDatabase db, String name )
    {
        for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            if ( name.equals( node.getProperty( "name", null ) ) )
                return node.getId();
        fail( "No node '" + name + "' found in " + db );
        return 0; // Never called
    }

    private long createNode( HighlyAvailableGraphDatabase db, String name )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            node.setProperty( "name", name );
            tx.success();
            return node.getId();
        }
        finally
        {
            tx.finish();
        }
    }
}
