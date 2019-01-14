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
package org.neo4j.index.internal.gbptree;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cursor.RawCursor;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class GBPTreeReadWriteTestBase<KEY,VALUE>
{
    private RandomRule random = new RandomRule();
    private PageCacheAndDependenciesRule deps = new PageCacheAndDependenciesRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( random ).around( deps );

    private TestLayout<KEY,VALUE> layout;
    private File indexFile;

    @Before
    public void setUp()
    {
        indexFile = deps.directory().file( "index" );
        layout = getLayout();
    }

    abstract TestLayout<KEY,VALUE> getLayout();

    @Test
    public void shouldSeeSimpleInsertions() throws Exception
    {
        try ( GBPTree<KEY,VALUE> index = index() )
        {
            int count = 1000;
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < count; i++ )
                {
                    writer.put( key( i ), value( i ) );
                }
            }

            try ( RawCursor<Hit<KEY,VALUE>,IOException> cursor = index.seek( key( 0 ), key( Long.MAX_VALUE ) ) )
            {
                for ( int i = 0; i < count; i++ )
                {
                    assertTrue( cursor.next() );
                    assertEqualsKey( key( i ), cursor.get().key() );
                }
                assertFalse( cursor.next() );
            }
        }
    }

    @Test
    public void shouldSeeSimpleInsertionsWithExactMatch() throws Exception
    {
        try ( GBPTree<KEY,VALUE> index = index() )
        {
            int count = 1000;
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < count; i++ )
                {
                    writer.put( key( i ), value( i ) );
                }
            }

            for ( int i = 0; i < count; i++ )
            {
                try ( RawCursor<Hit<KEY,VALUE>,IOException> cursor = index.seek( key( i ), key( i ) ) )
                {
                    assertTrue( cursor.next() );
                    assertEqualsKey( key( i ), cursor.get().key() );
                    assertFalse( cursor.next() );
                }
            }
        }
    }

    /* Randomized tests */

    @Test
    public void shouldSplitCorrectly() throws Exception
    {
        // GIVEN
        try ( GBPTree<KEY,VALUE> index = index() )
        {
            // WHEN
            int count = 1_000;
            List<KEY> seen = new ArrayList<>( count );
            try ( Writer<KEY,VALUE> writer = index.writer() )
            {
                for ( int i = 0; i < count; i++ )
                {
                    KEY key;
                    do
                    {
                        key = key( random.nextInt( 100_000 ) );
                    }
                    while ( listContains( seen, key ) );
                    VALUE value = value( i );
                    writer.put( key, value );
                    seen.add( key );
                }
            }

            // THEN
            try ( RawCursor<Hit<KEY,VALUE>,IOException> cursor = index.seek( key( 0 ), key( Long.MAX_VALUE ) ) )
            {
                long prev = -1;
                while ( cursor.next() )
                {
                    KEY hit = cursor.get().key();
                    long hitSeed = layout.keySeed( hit );
                    if ( hitSeed < prev )
                    {
                        fail( hit + " smaller than prev " + prev );
                    }
                    prev = hitSeed;
                    assertTrue( removeFromList( seen, hit ) );
                }

                if ( !seen.isEmpty() )
                {
                    fail( "expected hits " + seen );
                }
            }
        }
    }

    private GBPTree<KEY,VALUE> index() throws IOException
    {
        return new GBPTreeBuilder<>( deps.pageCache(), indexFile, layout ).build();
    }

    private boolean removeFromList( List<KEY> list, KEY item )
    {
        for ( int i = 0; i < list.size(); i++ )
        {
            if ( layout.compare( list.get( i ), item ) == 0 )
            {
                list.remove( i );
                return true;
            }
        }
        return false;
    }

    private boolean listContains( List<KEY> list, KEY item )
    {
        for ( KEY key : list )
        {
            if ( layout.compare( key, item ) == 0 )
            {
                return true;
            }
        }
        return false;
    }

    private VALUE value( long seed )
    {
        return layout.value( seed );
    }

    private KEY key( long seed )
    {
        return layout.key( seed );
    }

    private void assertEqualsKey( KEY expected, KEY actual )
    {
        assertTrue( String.format( "expected equal, expected=%s, actual=%s", expected.toString(), actual.toString() ),
                layout.compare( expected, actual ) == 0 );
    }
}
