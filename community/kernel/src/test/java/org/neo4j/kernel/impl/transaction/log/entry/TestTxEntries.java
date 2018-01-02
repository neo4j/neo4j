/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.test.EphemeralFileSystemRule.shutdownDbAction;

public class TestTxEntries
{
    private final File storeDir = new File("dir");
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    /*
     * Starts a JVM, executes a tx that fails on prepare and rollbacks,
     * triggering a bug where an extra start entry for that tx is written
     * in the xa log.
     */
    @Test
    public void testStartEntryWrittenOnceOnRollback() throws Exception
    {
        final GraphDatabaseService db = new TestGraphDatabaseFactory().setFileSystem( fs.get() ).newImpermanentDatabase( storeDir );
        createSomeTransactions( db );
        EphemeralFileSystemAbstraction snapshot = fs.snapshot( shutdownDbAction( db ) );

        new TestGraphDatabaseFactory().setFileSystem( snapshot ).newImpermanentDatabase( storeDir ).shutdown();
    }

    private void createSomeTransactions( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        Node node1 = db.createNode();
        Node node2 = db.createNode();
        node1.createRelationshipTo( node2,
                DynamicRelationshipType.withName( "relType1" ) );
        tx.success();
        tx.close();

        tx = db.beginTx();
        node1.delete();
        tx.success();
        try
        {
            // Will throw exception, causing the tx to be rolledback.
            tx.close();
        }
        catch ( Exception nothingToSeeHereMoveAlong )
        {
            // InvalidRecordException coming, node1 has rels
        }
        /*
         *  The damage has already been done. The following just makes sure
         *  the corrupting tx is flushed to disk, since we will exit
         *  uncleanly.
         */
        tx = db.beginTx();
        node1.setProperty( "foo", "bar" );
        tx.success();
        tx.close();
    }
}
