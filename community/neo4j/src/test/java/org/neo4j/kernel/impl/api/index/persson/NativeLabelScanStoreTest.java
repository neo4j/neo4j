package org.neo4j.kernel.impl.api.index.persson;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.storageengine.api.schema.LabelScanReader;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

public class NativeLabelScanStoreTest
{
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule( false );
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( getClass() );

    @Test
    public void shouldCompeteWithLucene() throws Exception
    {
        File storeDir = testDirectory.directory();
        final PageCache pageCache = pageCacheRule.getPageCache( new DefaultFileSystemAbstraction() );
        NativeLabelScanStore labelScanStore = new NativeLabelScanStore( pageCache, storeDir );

        final LabelScanWriter writer = labelScanStore.newWriter();
        for ( int id = 0; id < 10_000; id++ )
        {
            writer.write( NodeLabelUpdate.labelChanges( id, EMPTY_LONG_ARRAY, someLabels() ) );
        }

        final LabelScanReader reader = labelScanStore.newReader();
        for ( int labelId = 1; labelId <= 2; labelId++ )
        {
            final PrimitiveLongIterator primitiveLongIterator = reader.nodesWithLabel( labelId );
            assertThat( PrimitiveLongCollections.count( primitiveLongIterator ), equalTo( 10_000 ) );
        }
        labelScanStore.shutdown();
    }

    private long[] someLabels()
    {
        return new long[]{1L, 2L};
    }
}