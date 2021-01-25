/*
 * Copyright (c) "Neo4j"
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

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.ValueGroup;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

public class NativeValueIndexUtility<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
{
    private final ValueCreatorUtil<KEY,VALUE> valueCreatorUtil;
    private final Layout<KEY,VALUE> layout;

    public NativeValueIndexUtility( ValueCreatorUtil<KEY,VALUE> valueCreatorUtil, Layout<KEY,VALUE> layout )
    {
        this.valueCreatorUtil = valueCreatorUtil;
        this.layout = layout;
    }

    private void copyValue( VALUE value, VALUE intoValue )
    {
        valueCreatorUtil.copyValue( value, intoValue );
    }

    void verifyUpdates( ValueIndexEntryUpdate<IndexDescriptor>[] updates, Supplier<GBPTree<KEY,VALUE>> treeProvider )
            throws IOException
    {
        Pair<KEY,VALUE>[] expectedHits = convertToHits( updates, layout );
        List<Pair<KEY,VALUE>> actualHits = new ArrayList<>();
        try ( GBPTree<KEY,VALUE> tree = treeProvider.get();
              Seeker<KEY,VALUE> scan = scan( tree ) )
        {
            while ( scan.next() )
            {
                actualHits.add( deepCopy( scan ) );
            }
        }

        Comparator<Pair<KEY,VALUE>> hitComparator = ( h1, h2 ) ->
        {
            int keyCompare = layout.compare( h1.getKey(), h2.getKey() );
            if ( keyCompare == 0 )
            {
                return valueCreatorUtil.compareIndexedPropertyValue( h1.getKey(), h2.getKey() );
            }
            else
            {
                return keyCompare;
            }
        };
        assertSameHits( expectedHits, actualHits.toArray( new Pair[0] ), hitComparator );
    }

    private Seeker<KEY,VALUE> scan( GBPTree<KEY,VALUE> tree ) throws IOException
    {
        KEY lowest = layout.newKey();
        lowest.initialize( Long.MIN_VALUE );
        lowest.initValueAsLowest( 0, ValueGroup.UNKNOWN );
        KEY highest = layout.newKey();
        highest.initialize( Long.MAX_VALUE );
        highest.initValueAsHighest( 0, ValueGroup.UNKNOWN );
        return tree.seek( lowest, highest, NULL );
    }

    private void assertSameHits( Pair<KEY, VALUE>[] expectedHits, Pair<KEY, VALUE>[] actualHits,
            Comparator<Pair<KEY, VALUE>> comparator )
    {
        Arrays.sort( expectedHits, comparator );
        Arrays.sort( actualHits, comparator );
        assertEquals(
            expectedHits.length, actualHits.length, format( "Array length differ%nExpected:%d, Actual:%d",
                    expectedHits.length, actualHits.length ) );

        for ( int i = 0; i < expectedHits.length; i++ )
        {
            Pair<KEY,VALUE> expected = expectedHits[i];
            Pair<KEY,VALUE> actual = actualHits[i];
            assertEquals( 0, comparator.compare( expected, actual ),
                "Hits differ on item number " + i + ". Expected " + expected + " but was " + actual );
        }
    }

    private Pair<KEY,VALUE> deepCopy( Seeker<KEY,VALUE> from )
    {
        KEY intoKey = layout.newKey();
        VALUE intoValue = layout.newValue();
        layout.copyKey( from.key(), intoKey );
        copyValue( from.value(), intoValue );
        return Pair.of( intoKey, intoValue );
    }

    private Pair<KEY,VALUE>[] convertToHits( ValueIndexEntryUpdate<IndexDescriptor>[] updates,
            Layout<KEY,VALUE> layout )
    {
        List<Pair<KEY,VALUE>> hits = new ArrayList<>( updates.length );
        for ( ValueIndexEntryUpdate<IndexDescriptor> u : updates )
        {
            KEY key = layout.newKey();
            key.initialize( u.getEntityId() );
            for ( int i = 0; i < u.values().length; i++ )
            {
                key.initFromValue( i, u.values()[i], NEUTRAL );
            }
            VALUE value = layout.newValue();
            value.from( u.values() );
            hits.add( Pair.of( key, value ) );
        }
        return hits.toArray( new Pair[0] );
    }
}
