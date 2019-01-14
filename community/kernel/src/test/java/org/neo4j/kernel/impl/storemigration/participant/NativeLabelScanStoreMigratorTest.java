/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.storemigration.participant;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.scan.FullStoreChangeStream;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.impl.store.InvalidIdGeneratorException;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreFile;
import org.neo4j.kernel.impl.store.format.standard.StandardV2_3;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_2;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.util.monitoring.ProgressReporter;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NativeLabelScanStoreMigratorTest
{
    private final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( testDirectory ).around( fileSystemRule ).around( pageCacheRule );

    private File storeDir;
    private File nativeLabelIndex;
    private File migrationDir;
    private File luceneLabelScanStore;

    private final ProgressReporter progressReporter = mock( ProgressReporter.class );

    private FileSystemAbstraction fileSystem;
    private PageCache pageCache;
    private NativeLabelScanStoreMigrator indexMigrator;

    @Before
    public void setUp() throws Exception
    {
        storeDir = testDirectory.directory( "store" );
        nativeLabelIndex = new File( storeDir, NativeLabelScanStore.FILE_NAME );
        migrationDir = testDirectory.directory( "migrationDir" );
        luceneLabelScanStore = storeDir.toPath().resolve( Paths.get( "schema", "label", "lucene" ) ).toFile();

        fileSystem = fileSystemRule.get();
        pageCache = pageCacheRule.getPageCache( fileSystemRule );
        indexMigrator = new NativeLabelScanStoreMigrator( fileSystem, pageCache, Config.defaults() );
        fileSystem.mkdirs( luceneLabelScanStore );
    }

    @Test
    public void skipMigrationIfNativeIndexExist() throws Exception
    {
        ByteBuffer sourceBuffer = writeFile( nativeLabelIndex, new byte[]{1, 2, 3} );

        indexMigrator.migrate( storeDir, migrationDir, progressReporter, StandardV3_2.STORE_VERSION, StandardV3_2.STORE_VERSION );
        indexMigrator.moveMigratedFiles( migrationDir, storeDir, StandardV3_2.STORE_VERSION, StandardV3_2.STORE_VERSION );

        ByteBuffer resultBuffer = readFileContent( nativeLabelIndex, 3 );
        assertEquals( sourceBuffer, resultBuffer );
        assertTrue( fileSystem.fileExists( luceneLabelScanStore ) );
    }

    @Test( expected = InvalidIdGeneratorException.class )
    public void failMigrationWhenNodeIdFileIsBroken() throws Exception
    {
        prepareEmpty23Database();
        File nodeIdFile = new File( storeDir, StoreFile.NODE_STORE.storeFileName() + ".id" );
        writeFile( nodeIdFile, new byte[]{1, 2, 3} );

        indexMigrator.migrate( storeDir, migrationDir, progressReporter, StandardV3_2.STORE_VERSION, StandardV3_2.STORE_VERSION );
    }

    @Test
    public void clearMigrationDirFromAnyLabelScanStoreBeforeMigrating() throws Exception
    {
        // given
        prepareEmpty23Database();
        initializeNativeLabelScanStoreWithContent( migrationDir );
        File toBeDeleted = new File( migrationDir, NativeLabelScanStore.FILE_NAME );
        assertTrue( fileSystem.fileExists( toBeDeleted ) );

        // when
        indexMigrator.migrate( storeDir, migrationDir, progressReporter, StandardV3_2.STORE_VERSION, StandardV3_2.STORE_VERSION );

        // then
        assertNoContentInNativeLabelScanStore( migrationDir );
    }

    @Test
    public void luceneLabelIndexRemovedAfterSuccessfulMigration() throws IOException
    {
        prepareEmpty23Database();

        indexMigrator.migrate( storeDir, migrationDir, progressReporter, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );
        indexMigrator.moveMigratedFiles( migrationDir, storeDir, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );

        assertFalse( fileSystem.fileExists( luceneLabelScanStore ) );
    }

    @Test
    public void moveCreatedNativeLabelIndexBackToStoreDirectory() throws IOException
    {
        prepareEmpty23Database();
        indexMigrator.migrate( storeDir, migrationDir, progressReporter, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );
        File migrationNativeIndex = new File( migrationDir, NativeLabelScanStore.FILE_NAME );
        ByteBuffer migratedFileContent = writeFile( migrationNativeIndex, new byte[]{5, 4, 3, 2, 1} );

        indexMigrator.moveMigratedFiles( migrationDir, storeDir, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );

        ByteBuffer movedNativeIndex = readFileContent( nativeLabelIndex, 5 );
        assertEquals( migratedFileContent, movedNativeIndex );
    }

    @Test
    public void populateNativeLabelScanIndexDuringMigration() throws IOException
    {
        prepare34DatabaseWithNodes();
        indexMigrator.migrate( storeDir, migrationDir, progressReporter, StandardV3_4.STORE_VERSION, StandardV3_4.STORE_VERSION );
        indexMigrator.moveMigratedFiles( migrationDir, storeDir, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );

        try ( Lifespan lifespan = new Lifespan() )
        {
            NativeLabelScanStore labelScanStore = getNativeLabelScanStore( storeDir, true );
            lifespan.add( labelScanStore );
            for ( int labelId = 0; labelId < 10; labelId++ )
            {
                try ( LabelScanReader labelScanReader = labelScanStore.newReader() )
                {
                    int nodeCount = PrimitiveLongCollections.count( labelScanReader.nodesWithLabel( labelId ) );
                    assertEquals( format( "Expected to see only one node for label %d but was %d.", labelId, nodeCount ),
                            1, nodeCount );
                }
            }
        }
    }

    @Test
    public void reportProgressOnNativeIndexPopulation() throws IOException
    {
        prepare34DatabaseWithNodes();
        indexMigrator.migrate( storeDir, migrationDir, progressReporter, StandardV3_4.STORE_VERSION, StandardV3_4.STORE_VERSION );
        indexMigrator.moveMigratedFiles( migrationDir, storeDir, StandardV2_3.STORE_VERSION, StandardV3_2.STORE_VERSION );

        verify( progressReporter ).start( 10 );
        verify( progressReporter, times( 10 ) ).progress( 1 );
    }

    private NativeLabelScanStore getNativeLabelScanStore( File dir, boolean readOnly )
    {
        return new NativeLabelScanStore( pageCache, fileSystem, dir, FullStoreChangeStream.EMPTY, readOnly, new Monitors(),
                RecoveryCleanupWorkCollector.ignore() );
    }

    private void initializeNativeLabelScanStoreWithContent( File dir ) throws IOException
    {
        try ( Lifespan lifespan = new Lifespan() )
        {
            NativeLabelScanStore nativeLabelScanStore = getNativeLabelScanStore( dir, false );
            lifespan.add( nativeLabelScanStore );
            try ( LabelScanWriter labelScanWriter = nativeLabelScanStore.newWriter() )
            {
                labelScanWriter.write( NodeLabelUpdate.labelChanges( 1, new long[0], new long[]{1} ) );
            }
            nativeLabelScanStore.force( IOLimiter.unlimited() );
        }
    }

    private void assertNoContentInNativeLabelScanStore( File dir )
    {
        try ( Lifespan lifespan = new Lifespan() )
        {
            NativeLabelScanStore nativeLabelScanStore = getNativeLabelScanStore( dir, true );
            lifespan.add( nativeLabelScanStore );
            try ( LabelScanReader labelScanReader = nativeLabelScanStore.newReader() )
            {
                int count = PrimitiveLongCollections.count( labelScanReader.nodesWithLabel( 1 ) );
                assertEquals( 0, count );
            }
        }
    }

    private ByteBuffer writeFile( File file, byte[] content ) throws IOException
    {
        ByteBuffer sourceBuffer = ByteBuffer.wrap( content );
        storeFileContent( file, sourceBuffer );
        sourceBuffer.flip();
        return sourceBuffer;
    }

    private void prepare34DatabaseWithNodes()
    {
        GraphDatabaseService embeddedDatabase = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
        try
        {
            try ( Transaction transaction = embeddedDatabase.beginTx() )
            {
                for ( int i = 0; i < 10; i++ )
                {
                    embeddedDatabase.createNode( Label.label( "label" + i ) );
                }
                transaction.success();
            }
        }
        finally
        {
            embeddedDatabase.shutdown();
        }
        fileSystem.deleteFile( nativeLabelIndex );
    }

    private void prepareEmpty23Database() throws IOException
    {
        new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir ).shutdown();
        fileSystem.deleteFile( nativeLabelIndex );
        MetaDataStore.setRecord( pageCache, new File( storeDir, MetaDataStore.DEFAULT_NAME ), MetaDataStore.Position
                .STORE_VERSION, MetaDataStore.versionStringToLong( StandardV2_3.STORE_VERSION ) );
    }

    private ByteBuffer readFileContent( File nativeLabelIndex, int length ) throws IOException
    {
        try ( StoreChannel storeChannel = fileSystem.open( nativeLabelIndex, OpenMode.READ ) )
        {
            ByteBuffer readBuffer = ByteBuffer.allocate( length );
            //noinspection StatementWithEmptyBody
            while ( readBuffer.hasRemaining() && storeChannel.read( readBuffer ) > 0 )
            {
                // read till the end of store channel
            }
            readBuffer.flip();
            return readBuffer;
        }
    }

    private void storeFileContent( File file, ByteBuffer sourceBuffer ) throws IOException
    {
        try ( StoreChannel storeChannel = fileSystem.create( file ) )
        {
            storeChannel.writeAll( sourceBuffer );
        }
    }
}
