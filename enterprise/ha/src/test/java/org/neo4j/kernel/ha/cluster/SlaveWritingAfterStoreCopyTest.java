/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.cluster;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static org.junit.Assert.assertTrue;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

public class SlaveWritingAfterStoreCopyTest
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( this.getClass() );

    @Test
    public void shouldHandleSlaveWritingFirstAfterStoryCopy() throws Throwable
    {
        // Given
        Set<Long> expected = new HashSet();
        ClusterManager.ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        // When
        expected.add( createOneNode( master ) );
        cluster.sync();

        // ... crash the slave
        File slaveStoreDirectory = cluster.getStoreDir( slave );
        ClusterManager.RepairKit shutdownSlave = cluster.shutdown( slave );
        deleteRecursively( slaveStoreDirectory );

        // ... and slave copy store from master
        slave = shutdownSlave.repair();
        // ... and first write after crash occurs on salve
        expected.add( createOneNode( slave ) );
        cluster.sync();

        // Then
        assertTrue( expected.equals( collectIds( master ) ) );
        assertTrue( expected.equals( collectIds( slave ) ) );
    }

    private Set<Long> collectIds( HighlyAvailableGraphDatabase db )
    {
        Set<Long> result = new HashSet();
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : db.getAllNodes() )
            {
                result.add( node.getId() );
            }
            tx.success();
        }
        return result;
    }

    private long createOneNode( HighlyAvailableGraphDatabase db )
    {
        long id;
        try ( Transaction tx = db.beginTx() )
        {
            id = db.createNode().getId();
            tx.success();
        }
        return id;
    }
}
