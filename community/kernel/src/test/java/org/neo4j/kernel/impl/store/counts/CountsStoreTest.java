package org.neo4j.kernel.impl.store.counts;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.store.CommonAbstractStore.ALL_STORES_VERSION;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

public class CountsStoreTest
{
    @Test
    public void shouldCreateAnEmptyStore() throws IOException
    {
        // when
        CountsStore.createEmpty( pageCache, alpha, ALL_STORES_VERSION );
        try ( CountsStore counts = CountsStore.open( fs, pageCache, alpha ) )
        {
            // then
            assertEquals( 0, counts.get( CountsKey.nodeKey( 0 ) ) );
            assertEquals( 0, counts.get( CountsKey.relationshipKey( 1, 2, 3 ) ) );
            assertEquals( BASE_TX_ID, counts.lastTxId() );
            assertEquals( 0, counts.totalRecordsStored() );
            assertEquals( alpha, counts.file() );
            counts.accept( new RecordVisitor()
            {
                @Override
                public void visit( CountsKey key, long value )
                {
                    fail( "should not have been called" );
                }
            } );
        }
    }

    @Test
    public void shouldUpdateTheStore() throws IOException
    {
        // given
        CountsStore.createEmpty( pageCache, alpha, ALL_STORES_VERSION );
        CountsStore.Writer writer;
        try ( CountsStore counts = CountsStore.open( fs, pageCache, alpha ) )
        {
            // when
            writer = counts.newWriter( beta, lastCommittedTxId );
            writer.visit( CountsKey.nodeKey( 0 ), 21 );
            writer.visit( CountsKey.relationshipKey( 1, 2, 3 ), 32 );
            writer.close();
        }

        try ( CountsStore updated = writer.openForReading() )
        {
            // then
            assertEquals( 21, updated.get( CountsKey.nodeKey( 0 ) ) );
            assertEquals( 32, updated.get( CountsKey.relationshipKey( 1, 2, 3 ) ) );
            assertEquals( lastCommittedTxId, updated.lastTxId() );
            assertEquals( 2, updated.totalRecordsStored() );
            assertEquals( beta, updated.file() );
            updated.accept( new RecordVisitor()
            {
                @Override
                public void visit( CountsKey key, long value )
                {
                    key.accept( new CountsVisitor()
                    {
                        @Override
                        public void visitNodeCount( int labelId, long count )
                        {
                            assertEquals( 0, labelId );
                            assertEquals( 21, count );
                        }

                        @Override
                        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
                        {
                            assertEquals( 1, startLabelId );
                            assertEquals( 2, typeId );
                            assertEquals( 3, endLabelId );
                            assertEquals( 32, count );
                        }
                    }, value );
                }
            } );
        }
    }

    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    private final File alpha = new File( "alpha" );
    private final File beta = new File( "beta" );
    private final int lastCommittedTxId = 42;
    private FileSystemAbstraction fs;
    private PageCache pageCache;

    @Before
    public void setup()
    {
        fs = fsRule.get();
        pageCache = pageCacheRule.getPageCache( fs, new Config() );
    }
}
