package org.neo4j.index.bptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.neo4j.index.Modifier.Options.DEFAULTS;
import static org.neo4j.index.ValueAmenders.overwrite;

public class IndexModifierTest
{
    private final int pageSize = 512;

    private final SimpleIdProvider id = new SimpleIdProvider();
    private final Layout<MutableLong,MutableLong> layout = new SimpleLongLayout();
    private final TreeNode<MutableLong,MutableLong> node = new TreeNode<>( pageSize, layout );
    private final IndexModifier<MutableLong,MutableLong> indexModifier = new IndexModifier<>( id, node, layout );

    private final PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( pageSize );

    private final MutableLong insertKey = new MutableLong();
    private final MutableLong insertValue = new MutableLong();
    private final MutableLong readKey = new MutableLong();
    private final MutableLong readValue = new MutableLong();

    @Before
    public void setUp() throws IOException
    {
        id.reset();
        cursor.initialize();
        cursor.next();
        node.initializeLeaf( cursor );
    }

    @Test
    public void modifierMustInsertAtFirstPositionInEmptyLeaf() throws Exception
    {
        // given
        long key = 1L;
        long value = 1L;
        assertThat( node.keyCount( cursor ), is( 0 ) );

        // when
        insert( key, value );

        // then
        assertThat( node.keyCount( cursor ), is( 1 ) );
        assertThat( keyAt( 0 ), is( key ) );
        assertThat( valueAt( 0 ), is( key ) );
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertFirstInLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();

        for ( int i = 0; i < maxKeyCount; i++ )
        {
            // when
            long key = maxKeyCount - i;
            insert( key, key );

            // then
            assertThat( keyAt( 0 ), is( key ) );
            assertThat( valueAt( 0 ), is( key ) );
        }
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertLastInLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();

        for ( int i = 0; i < maxKeyCount; i++ )
        {
            // when
            insert( (long) i, (long) i );

            // then
            assertThat( keyAt( i ), is( (long) i ) );
            assertThat( valueAt( i ), is( (long) i ) );
        }
    }

    @Test
    public void modifierMustSortCorrectlyOnInsertInMiddleOfLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();

        for ( int i = 0; i < maxKeyCount; i++ )
        {
            // when
            long key = i % 2 == 0 ? i / 2 : maxKeyCount - i / 2;
            insert( key, key );

            // then
            assertThat( keyAt( (i + 1) / 2 ), is( key ) );
        }
    }

    @Test
    public void modifierMustSplitWhenInsertingMiddleOfFullLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();

        // when
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i % 2 == 0 ? i / 2 : maxKeyCount - i / 2;
            insert( key, key );
        }

        // then
        long middle = maxKeyCount / 2;
        assertNotNull( insert( middle, middle ) );
    }

    @Test
    public void modifierMustSplitWhenInsertingLastInFullLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();

        // when
        long key = 0;
        while ( key < maxKeyCount )
        {
            assertNull( insert( key, key ) );
            key++;
        }

        // then
        assertNotNull( insert( key, key ) ); // Should cause a split
    }

    @Test
    public void modifierMustSplitWhenInsertingFirstInFullLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();

        // when
        for ( int i = 0; i < maxKeyCount; i++ )
        {
            long key = i + 1;
            assertNull( insert( key, key ) );
        }

        // then
        assertNotNull( insert( 0L, 0L ) );
    }

    @Test
    public void modifierMustLeaveCursorOnSamePageAfterSplitInLeaf() throws Exception
    {
        // given
        int maxKeyCount = node.leafMaxKeyCount();
        long pageId = cursor.getCurrentPageId();
        long key = 0;
        while ( key < maxKeyCount )
        {
            assertNull( insert( key, key ) );
            key++;
        }

        // when
        assertNotNull( insert( key, key ) ); // Should cause a split

        // then
        assertThat( cursor.getCurrentPageId(), is( pageId ) );
    }

    private Long keyAt( int pos )
    {
        return node.keyAt( cursor, readKey, pos ).getValue();
    }

    private Long valueAt( int pos )
    {
        return node.valueAt( cursor, readValue, pos ).getValue();
    }

    private SplitResult<MutableLong> insert( long key, long value ) throws IOException
    {
        insertKey.setValue( key );
        insertValue.setValue( value );
        return indexModifier.insert( cursor, insertKey, insertValue, overwrite(), DEFAULTS );
    }

    private class SimpleIdProvider implements IdProvider
    {
        private long lastId = -1;

        @Override
        public long acquireNewId()
        {
            lastId++;
            return lastId;
        }

        private void reset()
        {
            lastId = -1;
        }
    }
}
