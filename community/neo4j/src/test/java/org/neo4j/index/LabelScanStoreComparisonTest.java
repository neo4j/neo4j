/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.labelscan.LabelScanIndex;
import org.neo4j.kernel.api.impl.labelscan.LuceneLabelScanIndexBuilder;
import org.neo4j.kernel.api.impl.labelscan.LuceneLabelScanStore;
import org.neo4j.kernel.api.impl.labelscan.storestrategy.BitmapDocumentFormat;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.FullStoreChangeStream;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.labelscan.NativeLabelScanStore;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.helpers.Format.duration;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.helpers.progress.ProgressMonitorFactory.textual;
import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.labelChanges;
import static org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider.EMPTY;

public class LabelScanStoreComparisonTest
{
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule( false );
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( getClass() ).keepDirectoryAfterSuccessfulTest();
    @Rule
    public final RandomRule random = new RandomRule();
    @Rule
    public final LifeRule life = new LifeRule( true );
    @Rule
    public final TestName testName = new TestName();
    private final int count = 100_000;
    private final int txSize = 100;

    @Test
    public void shouldTestNativeStore() throws Exception
    {
        File storeDir = testDirectory.directory();
        final PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction(),
                Config.defaults().augment( stringMap( GraphDatabaseSettings.pagecache_memory.name(), "50M" ) ) );
        LabelScanStore labelScanStore = life.add( new NativeLabelScanStore( pageCache, storeDir, Long.SIZE, 0, EMPTY ) );

        testLabelScanStore( labelScanStore );
    }

    @Test
    public void shouldTestLuceneStore() throws Exception
    {
        DirectoryFactory directoryFactory = DirectoryFactory.PERSISTENT;
        Config config = Config.empty();
        BitmapDocumentFormat documentFormat = BitmapDocumentFormat._64;
        PartitionedIndexStorage indexStorage = new PartitionedIndexStorage( directoryFactory,
                new DefaultFileSystemAbstraction(), testDirectory.directory(),
                LuceneLabelScanIndexBuilder.DEFAULT_INDEX_IDENTIFIER, false );
        LabelScanIndex index = LuceneLabelScanIndexBuilder.create()
                                .withDirectoryFactory( directoryFactory )
                                .withIndexStorage( indexStorage )
                                .withOperationalMode( OperationalMode.single )
                                .withConfig( config )
                                .withDocumentFormat( documentFormat )
                                .build();

        LabelScanStore store = life.add( new LuceneLabelScanStore( index, EMPTY_STORE, NullLogProvider.getInstance(),
                LuceneLabelScanStore.Monitor.EMPTY ) );
        testLabelScanStore( store );
    }

    private void testLabelScanStore( LabelScanStore labelScanStore ) throws IOException
    {
        // === WRITE ===
        long time = currentTimeMillis();
        ProgressListener progress = textual( System.out ).singlePart( "Insert:" + testName.getMethodName(), count );
//        writeSequential( labelScanStore, progress );
        writeRandomSmallTransactions( labelScanStore, progress );
        long writeTime = currentTimeMillis() - time;
        System.out.println( "write:" + duration( writeTime ) );

        // === READ ===
        time = currentTimeMillis();
        try ( final LabelScanReader reader = labelScanStore.newReader() )
        {
            for ( int labelId = 1; labelId <= 2; labelId++ )
            {
                final PrimitiveLongIterator nodeIds = reader.nodesWithLabel( labelId );
                PrimitiveLongCollections.count( nodeIds ); // to loop through it
            }
        }
        long readTime = currentTimeMillis() - time;
        System.out.println( "read:" + duration( readTime ) );
    }

    private void writeRandomSmallTransactions( LabelScanStore labelScanStore, ProgressListener progress ) throws IOException
    {
        // Random in txs of size 10
        int txs = count / txSize;
        long[] ids = new long[txSize];
        for ( int t = 0; t < txs; t++ )
        {
            for ( int i = 0; i < txSize; i++ )
            {
                ids[i] = random.nextInt( 100_000_000 );
            }
            Arrays.sort( ids );
            try ( LabelScanWriter writer = labelScanStore.newWriter() )
            {
                for ( int i = 0; i < ids.length; i++ )
                {
                    writer.write( labelChanges( ids[i], EMPTY_LONG_ARRAY, someLabels() ) );
                }
            }
            progress.add( txSize );
        }
    }

    private void writeSequential( LabelScanStore labelScanStore, ProgressListener progress ) throws IOException
    {
        // Sequential
        try ( final LabelScanWriter writer = labelScanStore.newWriter( true ) )
        {
            int batchSize = 1_000;
            int batches = count / batchSize;
            for ( int i = 0, id = 0; i < batches; i++ )
            {
                for ( int b = 0; b < batchSize; b++, id++ )
                {
                    writer.write( labelChanges( id, EMPTY_LONG_ARRAY, someLabels() ) );
                }
                progress.add( batchSize );
            }
        }
    }

    private long[] someLabels()
    {
        return new long[]{1L, 2L};
    }

    private static final FullStoreChangeStream EMPTY_STORE = new FullStoreChangeStream()
    {
        @Override
        public long applyTo( LabelScanWriter writer ) throws IOException
        {
            return 0;
        }
    };
}
