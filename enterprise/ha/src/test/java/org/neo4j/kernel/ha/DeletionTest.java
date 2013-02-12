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
package org.neo4j.kernel.ha;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

/**
 * Test for a regression:
 *
 * IndexOutOfBoundsException(-1) when applying a transaction that deletes relationship(s).
 * Happens when performing transactions in HA, or during recovery.
 *
 * Symptomatic stack trace:
 *
 * java.lang.IndexOutOfBoundsException: index -1
 *     at java.util.concurrent.atomic.AtomicReferenceArray.checkedByteOffset(AtomicReferenceArray.java:50)
 *     at java.util.concurrent.atomic.AtomicReferenceArray.get(AtomicReferenceArray.java:95)
 *     at org.neo4j.kernel.impl.cache.GCResistantCache.get(GCResistantCache.java:188)
 *     at org.neo4j.kernel.impl.core.NodeManager.invalidateNode(NodeManager.java:567)
 *     at org.neo4j.kernel.impl.core.NodeManager.patchDeletedRelationshipNodes(NodeManager.java:561)
 *     at org.neo4j.kernel.impl.core.WritableTransactionState.patchDeletedRelationshipNodes(WritableTransactionState.java:558)
 *     at org.neo4j.kernel.impl.nioneo.xa.Command$RelationshipCommand.removeFromCache(Command.java:432)
 *     at org.neo4j.kernel.impl.nioneo.xa.WriteTransaction.executeDeleted(WriteTransaction.java:562)
 *     at org.neo4j.kernel.impl.nioneo.xa.WriteTransaction.applyCommit(WriteTransaction.java:476)
 *     at org.neo4j.kernel.impl.nioneo.xa.WriteTransaction.doCommit(WriteTransaction.java:426)
 */
public class DeletionTest
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.forTest( getClass() ).cleanTestDirectory();

    /**
     * The problem would manifest even if the transaction was performed on the Master, it would then occur when the
     * Slave pulls updates and tries to apply the transaction. The reason for the test to run transactions against the
     * Slave is because it makes guarantees for when the master has to apply the transaction.
     */
    @Test
    public void shouldDeleteRecords() throws Exception
    {
        // given
        HighlyAvailableGraphDatabase db1 = start( testDirectory.directory().getPath() + "/db1", 0, stringMap() );
        HighlyAvailableGraphDatabase db2 = start( testDirectory.directory().getPath() + "/db2", 1, stringMap() );

        HighlyAvailableGraphDatabase master, slave;
        if ( db1.isMaster() )
        {
            master = db1;
            slave = db2;
        }
        else if ( db2.isMaster() )
        {
            master = db2;
            slave = db1;
        }
        else
        {
            throw new AssertionError( "NO MASTER" );
        }
        assertFalse( "Two masters", slave.isMaster() );

        Relationship rel;
        Transaction tx = slave.beginTx();
        try
        {
            rel = slave.createNode().createRelationshipTo( slave.createNode(), withName( "FOO" ) );

            tx.success();
        }
        finally
        {
            tx.finish();
        }

        assertNotNull( master.getRelationshipById( rel.getId() ) );

        // when
        tx = slave.beginTx();
        try
        {
            rel.delete();

            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // then - there should have been no exceptions
        slave.shutdown();
        master.shutdown();
    }

    private static HighlyAvailableGraphDatabase start( String storeDir, int i, Map<String, String> additionalConfig )
    {
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( storeDir )
                .setConfig( ClusterSettings.cluster_server, "127.0.0.1:" + (5001 + i) )
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                .setConfig( HaSettings.server_id, i + "" )
                .setConfig( HaSettings.ha_server, "127.0.0.1:" + (6666 + i) )
                .setConfig( HaSettings.pull_interval, "0ms" )
                .setConfig( additionalConfig )
                .newGraphDatabase();

        awaitStart( db );
        return db;
    }

    private static void awaitStart( HighlyAvailableGraphDatabase db )
    {
        db.beginTx().finish();
    }
}
