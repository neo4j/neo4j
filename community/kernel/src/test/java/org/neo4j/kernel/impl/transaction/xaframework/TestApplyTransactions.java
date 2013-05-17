/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.junit.Assert.assertEquals;

public class TestApplyTransactions
{
    @Test
    public void testCommittedTransactionReceivedAreForcedToLog() throws Exception
    {
        /* GIVEN
         * Create a tx on a db (as if the master), extract that, apply on dest (as if pullUpdate on slave).
         * Let slave crash uncleanly.
         */
        File baseStoreDir = new File( "base" );
        File originStoreDir = new File( baseStoreDir, "origin" );
        File destStoreDir = new File( baseStoreDir, "destination" );
        GraphDatabaseAPI origin = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( fs.get() )
                .newImpermanentDatabase( originStoreDir.getPath() );
        Transaction tx = origin.beginTx();
        origin.createNode();
        tx.success();
        tx.finish();
        XaDataSource originNeoDataSource = origin.getXaDataSourceManager().getXaDataSource(
                NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
        int latestTxId = (int) originNeoDataSource.getLastCommittedTxId();
        InMemoryLogBuffer theTx = new InMemoryLogBuffer();
        originNeoDataSource.getLogExtractor( latestTxId, latestTxId ).extractNext( theTx );

        final GraphDatabaseAPI dest = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( fs.get() )
                .newImpermanentDatabase( destStoreDir.getPath() );
        XaDataSource destNeoDataSource = dest.getXaDataSourceManager().getXaDataSource(
                NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
        destNeoDataSource.applyCommittedTransaction( latestTxId, theTx );
        origin.shutdown();
        EphemeralFileSystemAbstraction snapshot = fs.snapshot( new Runnable()
        {
            @Override
            public void run()
            {
                dest.shutdown();
            }
        } );

        /*
         * Open crashed db, try to extract the transaction it reports as latest. It should be there.
         */
        GraphDatabaseAPI newDest = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( snapshot )
                .newImpermanentDatabase( destStoreDir.getPath() );
        destNeoDataSource = newDest.getXaDataSourceManager().getXaDataSource(
                NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
        latestTxId = (int) destNeoDataSource.getLastCommittedTxId();
        theTx = new InMemoryLogBuffer();
        long extractedTxId = destNeoDataSource.getLogExtractor( latestTxId, latestTxId ).extractNext( theTx );
        assertEquals( latestTxId, extractedTxId );
    }
    
    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
