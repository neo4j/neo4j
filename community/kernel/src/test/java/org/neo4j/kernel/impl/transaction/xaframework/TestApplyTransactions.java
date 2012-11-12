/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.ProcessStreamHandler;

public class TestApplyTransactions
{
    @Test
    public void testCommittedTransactionReceivedAreForcedToLog() throws Exception
    {
        File baseStoreDir = new File( new File( "test-data" ), "test-apply-transaction" );
        Process process = Runtime.getRuntime().exec(
                new String[] { "java", "-cp", System.getProperty( "java.class.path" ),
                        TestApplyTransactions.class.getName(), baseStoreDir.getAbsolutePath() } );
        int exit = new ProcessStreamHandler( process, false ).waitForResult();
        assertEquals( 0, exit );

        /*
         * Open crashed db, try to extract the transaction it reports as latest. It should be there.
         */
        File destStoreDir = new File( baseStoreDir, "destination" );
        EmbeddedGraphDatabase dest = new EmbeddedGraphDatabase( destStoreDir.getAbsolutePath() );
        XaDataSource destNeoDataSource = dest.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                Config.DEFAULT_DATA_SOURCE_NAME );
        int latestTxId = (int) destNeoDataSource.getLastCommittedTxId();
        InMemoryLogBuffer theTx = new InMemoryLogBuffer();
        long extractedTxId = destNeoDataSource.getLogExtractor( latestTxId, latestTxId ).extractNext( theTx );
        assertEquals( latestTxId, extractedTxId );
    }

    public static void main( String[] args ) throws Exception
    {
        /*
         * Create a tx on a db (as if the master), extract that, apply on dest (as if pullUpdate on slave).
         * Let slave crash uncleanly.
         */
        File baseStoreDir = new File( args[0] );
        File originStoreDir = new File( baseStoreDir, "origin" );
        File destStoreDir = new File( baseStoreDir, "destination" );
        EmbeddedGraphDatabase origin = new EmbeddedGraphDatabase( originStoreDir.getAbsolutePath() );
        Transaction tx = origin.beginTx();
        origin.createNode();
        tx.success();
        tx.finish();
        XaDataSource originNeoDataSource = origin.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                Config.DEFAULT_DATA_SOURCE_NAME );
        int latestTxId = (int) originNeoDataSource.getLastCommittedTxId();
        System.out.println( "Xtracted tx id " + latestTxId );
        InMemoryLogBuffer theTx = new InMemoryLogBuffer();
        originNeoDataSource.getLogExtractor( latestTxId, latestTxId ).extractNext( theTx );

        EmbeddedGraphDatabase dest = new EmbeddedGraphDatabase( destStoreDir.getAbsolutePath() );
        XaDataSource destNeoDataSource = dest.getConfig().getTxModule().getXaDataSourceManager().getXaDataSource(
                Config.DEFAULT_DATA_SOURCE_NAME );
        destNeoDataSource.applyCommittedTransaction( latestTxId, theTx );

        origin.shutdown();
        // dest on purpose left to crash
    }
}
