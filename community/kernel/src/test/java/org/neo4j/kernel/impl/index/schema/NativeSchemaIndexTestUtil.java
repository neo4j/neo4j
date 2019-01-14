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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.neo4j.cursor.RawCursor;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Hit;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_WRITER;
import static org.neo4j.test.rule.PageCacheRule.config;

public abstract class NativeSchemaIndexTestUtil<KEY extends NativeSchemaKey<KEY>,VALUE extends NativeSchemaValue>
{
    static final long NON_EXISTENT_ENTITY_ID = 1_000_000_000;

    final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule( config().withAccessChecks( true ) );
    protected final RandomRule random = new RandomRule();
    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule ).around( random );

    SchemaIndexDescriptor schemaIndexDescriptor;
    LayoutTestUtil<KEY,VALUE> layoutUtil;
    Layout<KEY,VALUE> layout;
    private File indexFile;
    PageCache pageCache;
    IndexProvider.Monitor monitor = IndexProvider.Monitor.EMPTY;
    long indexId = 1;

    @Before
    public void setup()
    {
        layoutUtil = createLayoutTestUtil();
        schemaIndexDescriptor = layoutUtil.indexDescriptor();
        layout = layoutUtil.createLayout();
        indexFile = directory.file( "index" );
        pageCache = pageCacheRule.getPageCache( fs );
    }

    public File getIndexFile()
    {
        return indexFile;
    }

    abstract LayoutTestUtil<KEY,VALUE> createLayoutTestUtil();

    private void copyValue( VALUE value, VALUE intoValue )
    {
        layoutUtil.copyValue( value, intoValue );
    }

    void verifyUpdates( IndexEntryUpdate<SchemaIndexDescriptor>[] updates )
            throws IOException
    {
        Hit<KEY,VALUE>[] expectedHits = convertToHits( updates, layout );
        List<Hit<KEY,VALUE>> actualHits = new ArrayList<>();
        try ( GBPTree<KEY,VALUE> tree = getTree();
              RawCursor<Hit<KEY,VALUE>,IOException> scan = scan( tree ) )
        {
            while ( scan.next() )
            {
                actualHits.add( deepCopy( scan.get() ) );
            }
        }

        Comparator<Hit<KEY,VALUE>> hitComparator = ( h1, h2 ) ->
        {
            int keyCompare = layout.compare( h1.key(), h2.key() );
            if ( keyCompare == 0 )
            {
                return layoutUtil.compareIndexedPropertyValue( h1.key(), h2.key() );
            }
            else
            {
                return keyCompare;
            }
        };
        assertSameHits( expectedHits, actualHits.toArray( new Hit[0] ), hitComparator );
    }

    GBPTree<KEY,VALUE> getTree() throws IOException
    {
        return new GBPTree<>( pageCache, getIndexFile(), layout, 0, GBPTree.NO_MONITOR,
                NO_HEADER_READER, NO_HEADER_WRITER, RecoveryCleanupWorkCollector.immediate() );
    }

    private RawCursor<Hit<KEY,VALUE>, IOException> scan( GBPTree<KEY,VALUE> tree ) throws IOException
    {
        KEY lowest = layout.newKey();
        lowest.initAsLowest();
        KEY highest = layout.newKey();
        highest.initAsHighest();
        return tree.seek( lowest, highest );
    }

    private void assertSameHits( Hit<KEY, VALUE>[] expectedHits, Hit<KEY, VALUE>[] actualHits,
            Comparator<Hit<KEY, VALUE>> comparator )
    {
        Arrays.sort( expectedHits, comparator );
        Arrays.sort( actualHits, comparator );
        assertEquals( format( "Array length differ%nExpected:%s%nActual:%s",
                Arrays.toString( expectedHits ), Arrays.toString( actualHits ) ),
                expectedHits.length, actualHits.length );

        for ( int i = 0; i < expectedHits.length; i++ )
        {
            Hit<KEY,VALUE> expected = expectedHits[i];
            Hit<KEY,VALUE> actual = actualHits[i];
            assertTrue( "Hits differ on item number " + i + ". Expected " + expected + " but was " + actual,
                    comparator.compare( expected, actual ) == 0 );
        }
    }

    private Hit<KEY,VALUE> deepCopy( Hit<KEY,VALUE> from )
    {
        KEY intoKey = layout.newKey();
        VALUE intoValue = layout.newValue();
        layout.copyKey( from.key(), intoKey );
        copyValue( from.value(), intoValue );
        return new SimpleHit<>( intoKey, intoValue );
    }

    private Hit<KEY,VALUE>[] convertToHits( IndexEntryUpdate<SchemaIndexDescriptor>[] updates,
            Layout<KEY,VALUE> layout )
    {
        List<Hit<KEY,VALUE>> hits = new ArrayList<>( updates.length );
        for ( IndexEntryUpdate<SchemaIndexDescriptor> u : updates )
        {
            KEY key = layout.newKey();
            key.from( u.getEntityId(), u.values() );
            VALUE value = layout.newValue();
            value.from( u.values() );
            hits.add( hit( key, value ) );
        }
        return hits.toArray( new Hit[0] );
    }

    private Hit<KEY,VALUE> hit( final KEY key, final VALUE value )
    {
        return new SimpleHit<>( key, value );
    }

    void assertFilePresent()
    {
        assertTrue( fs.fileExists( getIndexFile() ) );
    }

    void assertFileNotPresent()
    {
        assertFalse( fs.fileExists( getIndexFile() ) );
    }
}
