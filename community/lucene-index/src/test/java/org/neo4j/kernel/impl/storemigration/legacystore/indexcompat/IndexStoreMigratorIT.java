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
package org.neo4j.kernel.impl.storemigration.legacystore.indexcompat;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.TimeZone;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.index.impl.lucene.LuceneCommandFactory;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.MigrationTestUtils;
import org.neo4j.kernel.impl.storemigration.StoreMigrator;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.MigrationProgressMonitor;
import org.neo4j.kernel.impl.storemigration.monitoring.SilentMigrationProgressMonitor;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.LogIoUtils;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommandFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.Unzip;

import static org.hamcrest.Matchers.array;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @see org.neo4j.kernel.impl.storemigration.StoreMigratorIT for related tests
 */
public class IndexStoreMigratorIT
{
    @Test
    public void shouldMigrateLuceneTransactionLogs() throws Exception
    {
        // GIVEN
        // A store that has lucene transaction logs
        File unzippedStore = Unzip.unzip( LegacyStore.class, "legacy-store-with-lucene-logs.zip" );
        File legacyStoreDir = new File( unzippedStore, "graph.db" );
        LegacyStore legacyStore = new LegacyStore( fs, new File( legacyStoreDir, NeoStore.DEFAULT_NAME ) );
        NeoStore neoStore = storeFactory.createNeoStore( storeFileName );

        // WHEN
        new StoreMigrator( monitor ).migrate( legacyStore, neoStore );
        legacyStore.close();

        // THEN
        // We expect to be able to read a command log that looks like this:
        //  === graph.db/index/lucene.log.v1 ===
        //      Logical log version: 1 with prev committed tx[3]
        //  Start[2,xid=GlobalId[NEOKERNL|3656877539012568704|0|-1], BranchId[ 49 54 50 51 55 52 ],master=-1,me=-1,time=2014-07-02 14:02:38.301+0000/1404309758301, checksum=-160182410641]
        //  Command[2, Add[Index[node_auto_index,Node],4,name,c]]
        //  Command[2, Add[Index[node_auto_index,Node],4,age,1]]
        //  Prepare[2, 2014-07-02 14:02:38.323+0000/1404309758323]
        //  2PC[2, txId=4, 2014-07-02 14:02:38.326+0000/1404309758326]
        //  Done[2]

        XaCommandFactory commandFactory = new LuceneCommandFactory( null );
        long txId = 4;
        long endTxId = 4;
        ByteBuffer buf = ByteBuffer.allocate( 100000 );
        File logFile = new File( storeDir, "index/lucene.log.v1" );
        StoreChannel channel = fs.open( logFile, "r" );

        long[] header = LogIoUtils.readLogHeader( buf, channel, true );
        assertThat( header[0], is( 1L ) ); // the lock version
        assertThat( header[1], is( 3L ) ); // the id of the last transaction in the previous log

        // The first transaction must have the correct checksum
        LogEntry startEntry = LogIoUtils.readEntry( buf, channel, commandFactory );
        LogEntry firstCommandEntry = LogIoUtils.readEntry( buf, channel, commandFactory );
        LogEntry secondCommandEntry = LogIoUtils.readEntry( buf, channel, commandFactory );
        LogEntry prepareEntry = LogIoUtils.readEntry( buf, channel, commandFactory );
        LogEntry twoPcEntry = LogIoUtils.readEntry( buf, channel, commandFactory );
        LogEntry doneEntry = LogIoUtils.readEntry( buf, channel, commandFactory );
        assertThat(
                toStrings( startEntry,
                        firstCommandEntry,
                        secondCommandEntry,
                        prepareEntry,
                        twoPcEntry,
                        doneEntry ),
                array( containsString(
                                "Start[2,xid=GlobalId[NEOKERNL|3656877539012568704|0|-1], " +
                                "BranchId[ 49 54 50 51 55 52 ],master=-1,me=-1,time=2014-07-02" ),
                        containsString( "Command[2, Add[Index[node_auto_index,Node],4,name,c]]" ),
                        containsString( "Command[2, Add[Index[node_auto_index,Node],4,age,1]]" ),
                        containsString( "Prepare[2, 2014-07-02" ),
                        containsString( "2PC[2, txId=4, 2014-07-02" ),
                        containsString( "Done[2]" ) ) );
    }

    private String[] toStrings( LogEntry... entries )
    {
        TimeZone utc = TimeZone.getTimeZone( "UTC" );
        String[] result = new String[ entries.length ];
        for ( int i = 0; i < entries.length; i++ )
        {
            result[i] = entries[i].toString( utc );
        }
        return result;
    }

    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
    private final String storeDir = TargetDirectory.forTest( getClass() ).makeGraphDbDir().getAbsolutePath();
    private final MigrationProgressMonitor monitor = new SilentMigrationProgressMonitor();
    private StoreFactory storeFactory;
    private File storeFileName;

    @Before
    public void setUp()
    {
        Config config = MigrationTestUtils.defaultConfig();
        File outputDir = new File( storeDir );
        storeFileName = new File( outputDir, NeoStore.DEFAULT_NAME );
        storeFactory = new StoreFactory( config, new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fs, StringLogger.DEV_NULL, new DefaultTxHook() );
    }
}
