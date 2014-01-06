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
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Ignore;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.TwoPhaseCommit;
import org.neo4j.test.LogTestUtils;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.TargetDirectory;

import static java.lang.Integer.parseInt;
import static java.lang.System.exit;

import static org.junit.Assert.assertEquals;

import static org.neo4j.test.LogTestUtils.EVERYTHING_BUT_DONE_RECORDS;
import static org.neo4j.test.LogTestUtils.filterNeostoreLogicalLog;
import static org.neo4j.test.LogTestUtils.filterTxLog;

@Ignore( "Used from another test case and is not a test case in itself" )
public class CreateTransactionsAndDie
{
    public static void main( String[] args )
    {
        String storeDir = args[0];
        int count = parseInt( args[1] );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        for ( int i = 0; i < 2; i++ ) create1pcTx( db );
        for ( int i = 0; i < count; i++ ) create2pcTx( db );
        
        // Intentionally don't shutdown the db cleanly
        exit( 0 );
    }

    private static void create1pcTx( GraphDatabaseService db )
    {
        try(Transaction tx = db.beginTx())
        {
            db.createNode();
            tx.success();
        }
    }

    private static void create2pcTx( GraphDatabaseService db )
    {
        try(Transaction tx = db.beginTx())
        {
            Node node = db.createNode();
            db.index().forNodes( "index" ).add( node, "key", "value" );
            tx.success();
        }
    }
    
    public static String produceNonCleanDbWhichWillRecover2PCsOnStartup( String name, int nrOf2PcTransactionsToRecover )
            throws Exception
    {
        FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
        String dir = TargetDirectory.forTest( CreateTransactionsAndDie.class ).directory( name, true ).getAbsolutePath();
        assertEquals( 0, createUncleanDb( dir, nrOf2PcTransactionsToRecover ) );
        filterTxLog( fileSystem, dir, EVERYTHING_BUT_DONE_RECORDS );
        remove2PCAndDoneFromLog( fileSystem, dir );
        // Here we have produced a state which looks like a couple of 2PC transactions
        // that are marked as committed, but not actually committed.
        return dir;
    }

    private static int createUncleanDb( String dir, int nrOf2PcTransactionsToRecover ) throws Exception
    {
        Process process = Runtime.getRuntime().exec( new String[]{
            "java", "-cp", System.getProperty( "java.class.path" ), CreateTransactionsAndDie.class.getName(),
            dir, "" + nrOf2PcTransactionsToRecover
        } );

        return new ProcessStreamHandler( process, true ).waitForResult();
    }

    private static void remove2PCAndDoneFromLog( FileSystemAbstraction fileSystem, String dir ) throws IOException
    {
        filterNeostoreLogicalLog( fileSystem, dir, new LogTestUtils.LogHook<LogEntry>()
        {
            private final Set<Integer> prune = new HashSet<Integer>();
            
            @Override
            public boolean accept( LogEntry item )
            {
                if ( item instanceof TwoPhaseCommit )
                {
                    prune.add( item.getIdentifier() );
                    return false;
                }
                else if ( prune.contains( item.getIdentifier() ) ) return false;
                return true;
            }

            @Override
            public void file( File file )
            {
            }

            @Override
            public void done( File file )
            {
            }
        } );
    }
}
