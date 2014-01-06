/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package recovery;

import java.io.File;

import org.junit.After;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.test.LogTestUtils.LogHookAdapter;
import org.neo4j.test.TargetDirectory;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.getProperty;

import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.util.FileUtils.truncateFile;
import static org.neo4j.test.LogTestUtils.filterNeostoreLogicalLog;

public class TestRecoveryNotHappening
{
    private final File storeDirectory = TargetDirectory.forTest( getClass() ).graphDbDir( true );
    private GraphDatabaseService db;
    
    /* So this test is here to assert that even if we have a scenario where
     * there's a transaction which is started and prepared, but lacking
     * commit or done, also unknown to the tx log, the store still recovers.
     * 
     * The main problem was that first each data source recovered what it could,
     * but kept any 2PC transactions that it didn't know what to do with around
     * for the tx manager, later during the recovery process, telling them what
     * to do with. If the tx manager (the tx log it has) didn't have any
     * recollection of any such a transaction it wouldn't even notify that data
     * source. The result would be that the database started, but one or more
     * data sources would be in a state where it was awaiting recovery to complete.
     * 
     * The particular case bringing this up was a transaction that failed to write
     * the 2PC log entry, due to the thread being interrupted, but the transaction
     * was successfully rolled back after that.
     */
    @Test
    public void uncompletedPreparedTransactionUnknownToTxLogWontPreventRecovery() throws Exception
    {
        // Given
        //  * a log containing a START and PREPARE entry
        //  * an empty tx log
        create2PCTransactionAndShutDownNonClean();
        modifyTransactionMakingItLookPreparedAndUncompleted();
        
        // When
        //  * starting (and recovering) this store should not result in exception
        startDb();
        
        // Then
        //  * it should have started and recovered properly and be able to handle requests
        createNodeWithNameProperty( db, "Everything is OK" );
    }

    private void create2PCTransactionAndShutDownNonClean() throws Exception
    {
        assertEquals( 0, getRuntime().exec( new String[] { "java", "-cp", getProperty( "java.class.path" ),
                getClass().getName(), storeDirectory.getAbsolutePath() } ).waitFor() );
    }
    
    private void modifyTransactionMakingItLookPreparedAndUncompleted() throws Exception
    {
        filterNeostoreLogicalLog( new DefaultFileSystemAbstraction(), storeDirectory.getAbsolutePath(), new LogHookAdapter<LogEntry>()
        {
            @Override
            public boolean accept( LogEntry item )
            {
                return !(item instanceof LogEntry.Done) && !(item instanceof LogEntry.TwoPhaseCommit);
            }
        } );
        truncateFile( new File( storeDirectory, "tm_tx_log.1" ), 0 );
    }
    
    public static void main( String[] args )
    {
        String storeDir = args[0];
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        createNodeWithNameProperty( db, "test" );
        System.exit( 0 );
    }

    private void startDb()
    {
        db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDirectory.getAbsolutePath() );
    }

    private static Node createNodeWithNameProperty( GraphDatabaseService db, String name )
    {
        try(Transaction tx = db.beginTx())
        {
            Node node = db.createNode();
            node.setProperty( "name", name );
            db.index().forNodes( "index" ).add( node, "name", name );
            tx.success();
            return node;
        }
    }

    @After
    public void doAfter()
    {
        if ( db != null )
            db.shutdown();
    }
}
