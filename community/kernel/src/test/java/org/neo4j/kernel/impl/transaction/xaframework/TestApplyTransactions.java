/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.io.IOException;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.LogTestUtils;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.test.EphemeralFileSystemRule.shutdownDb;
import static org.neo4j.kernel.impl.transaction.xaframework.LogMatchers.logEntries;
import static org.neo4j.test.LogTestUtils.filterNeostoreLogicalLog;

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
        XaDataSource originNeoDataSource = xaDs( origin );
        int latestTxId = (int) originNeoDataSource.getLastCommittedTxId();
        InMemoryLogBuffer theTx = new InMemoryLogBuffer();
        originNeoDataSource.getLogExtractor( latestTxId, latestTxId ).extractNext( theTx );

        final GraphDatabaseAPI dest = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( fs.get() )
                .newImpermanentDatabase( destStoreDir.getPath() );
        XaDataSource destNeoDataSource = xaDs( dest );
        destNeoDataSource.applyCommittedTransaction( latestTxId, theTx );
        origin.shutdown();
        EphemeralFileSystemAbstraction snapshot = fs.snapshot( shutdownDb( dest ) );

        /*
         * Open crashed db, try to extract the transaction it reports as latest. It should be there.
         */
        GraphDatabaseAPI newDest = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( snapshot )
                .newImpermanentDatabase( destStoreDir.getPath() );
        destNeoDataSource = newDest.getDependencyResolver().resolveDependency( XaDataSourceManager.class )
                .getXaDataSource( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
        latestTxId = (int) destNeoDataSource.getLastCommittedTxId();
        theTx = new InMemoryLogBuffer();
        long extractedTxId = destNeoDataSource.getLogExtractor( latestTxId, latestTxId ).extractNext( theTx );
        assertEquals( latestTxId, extractedTxId );
    }

    private XaDataSource xaDs( GraphDatabaseAPI origin )
    {
        return origin.getDependencyResolver().resolveDependency( XaDataSourceManager.class ).getXaDataSource(
                NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );
    }

    @Test
    public void verifyThatRecoveredTransactionsHaveTheirDoneRecordsWrittenInOrder() throws IOException
    {
        XaDataSource ds;
        File archivedLogFilename;

        File originStoreDir = new File( new File( "base" ), "origin" );
        String logicalLogFilename = "logicallog";

        final GraphDatabaseAPI db1 = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                        .setFileSystem( fs.get() )
                        .newImpermanentDatabaseBuilder( originStoreDir.getPath() )
                        .setConfig( InternalAbstractGraphDatabase.Configuration.logical_log, logicalLogFilename )
                        .newGraphDatabase();

        for ( int i = 0; i < 100; i++ )
        {
            Transaction tx = db1.beginTx();
            db1.createNode();
            tx.success();
            tx.finish();
        }

        ds = xaDs( db1 );
        archivedLogFilename = ds.getFileName( ds.getCurrentLogVersion() );

        fs.snapshot( new Runnable()
        {
            @Override
            public void run()
            {
                db1.shutdown();
            }
        } );

        removeDoneEntriesFromLog( new File( archivedLogFilename.getParent(), logicalLogFilename + ".1" ) );

        GraphDatabaseAPI db2 = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                        .setFileSystem( fs.get() )
                        .newImpermanentDatabaseBuilder( originStoreDir.getPath() )
                        .setConfig( InternalAbstractGraphDatabase.Configuration.logical_log, logicalLogFilename )
                        .newGraphDatabase();

        ds = xaDs( db2 );
        archivedLogFilename = ds.getFileName( ds.getCurrentLogVersion() );
        db2.shutdown();

        List<LogEntry> logEntries = filterDoneEntries( logEntries( fs.get(), archivedLogFilename ) );
        String errorMessage = "DONE entries should be in order: " + logEntries;
        int prev = 0;
        for ( LogEntry entry : logEntries )
        {
            int current = entry.getIdentifier();
            assertThat( errorMessage, current, greaterThan( prev ) );
            prev = current;
        }
    }

    private void removeDoneEntriesFromLog( File archivedLogFilename ) throws IOException
    {
        LogTestUtils.LogHook<LogEntry> doneEntryFilter = new LogTestUtils.LogHookAdapter<LogEntry>()
        {
            @Override
            public boolean accept( LogEntry item )
            {
                return !(item instanceof LogEntry.Done);
            }
        };
        EphemeralFileSystemAbstraction fsa = fs.get();
        File tempFile = filterNeostoreLogicalLog( fsa, archivedLogFilename, doneEntryFilter );
        fsa.deleteFile( archivedLogFilename );
        fsa.renameFile( tempFile, archivedLogFilename );
    }

    private List<LogEntry> filterDoneEntries( List<LogEntry> logEntries )
    {
        Predicate<? super LogEntry> doneEntryPredicate = new Predicate<LogEntry>()
        {
            @Override
            public boolean accept( LogEntry item )
            {
                return item instanceof LogEntry.Done;
            }
        };
        return Iterables.toList( Iterables.filter( doneEntryPredicate, logEntries ) );
    }

    @Rule public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
}
