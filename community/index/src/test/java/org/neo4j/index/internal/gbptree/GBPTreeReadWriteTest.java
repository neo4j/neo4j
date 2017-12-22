/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.cursor.RawCursor;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GBPTreeReadWriteTest
{
    private RandomRule random = new RandomRule();
    private PageCacheAndDependenciesRule deps = new PageCacheAndDependenciesRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( random ).around( deps );

    private Layout<MutableLong,MutableLong> layout;
    private File indexFile;

    @Before
    public void setUp()
    {
        indexFile = deps.directory().file( "index" );
        layout = getLayout();
    }

    private Layout<MutableLong,MutableLong> getLayout()
    {
        return new SimpleLongLayout();
    }

    @Test
    public void shouldSeeSimpleInsertions() throws Exception
    {
        try ( GBPTree<MutableLong,MutableLong> index = index() )
        {
            int count = 1000;
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                for ( int i = 0; i < count; i++ )
                {
                    writer.put( new MutableLong( i ), new MutableLong( i ) );
                }
            }

            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                          index.seek( new MutableLong( 0 ), new MutableLong( Long.MAX_VALUE ) ) )
            {
                for ( int i = 0; i < count; i++ )
                {
                    assertTrue( cursor.next() );
                    assertEquals( i, cursor.get().key().longValue() );
                }
                assertFalse( cursor.next() );
            }
        }
    }

    @Test
    public void shouldSeeSimpleInsertionsWithExactMatch() throws Exception
    {
        try ( GBPTree<MutableLong,MutableLong> index = index() )
        {
            int count = 1000;
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                for ( int i = 0; i < count; i++ )
                {
                    writer.put( new MutableLong( i ), new MutableLong( i ) );
                }
            }

            for ( int i = 0; i < count; i++ )
            {
                try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                              index.seek( new MutableLong( i ), new MutableLong( i ) ) )
                {
                    assertTrue( cursor.next() );
                    assertEquals( i, cursor.get().key().longValue() );
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
        try ( GBPTree<MutableLong,MutableLong> index = index() )
        {
            // WHEN
            int count = 1_000;
            PrimitiveLongSet seen = Primitive.longSet( count );
            try ( Writer<MutableLong,MutableLong> writer = index.writer() )
            {
                for ( int i = 0; i < count; i++ )
                {
                    MutableLong key;
                    do
                    {
                        key = new MutableLong( random.nextInt( 100_000 ) );
                    }
                    while ( !seen.add( key.longValue() ) );
                    MutableLong value = new MutableLong( i );
                    writer.put( key, value );
                    seen.add( key.longValue() );
                }
            }

            // THEN
            try ( RawCursor<Hit<MutableLong,MutableLong>,IOException> cursor =
                          index.seek( new MutableLong( 0 ), new MutableLong( Long.MAX_VALUE ) ) )
            {
                long prev = -1;
                while ( cursor.next() )
                {
                    MutableLong hit = cursor.get().key();
                    if ( hit.longValue() < prev )
                    {
                        fail( hit + " smaller than prev " + prev );
                    }
                    prev = hit.longValue();
                    assertTrue( seen.remove( hit.longValue() ) );
                }

                if ( !seen.isEmpty() )
                {
                    fail( "expected hits " + Arrays.toString( PrimitiveLongCollections.asArray( seen.iterator() ) ) );
                }
            }
        }
    }

    private GBPTree<MutableLong,MutableLong> index() throws IOException
    {
        return new GBPTreeBuilder<>( deps.pageCache(), indexFile, layout ).build();
    }
}
