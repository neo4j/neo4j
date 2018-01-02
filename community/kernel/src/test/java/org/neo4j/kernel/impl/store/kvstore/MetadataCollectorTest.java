/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.kvstore;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class MetadataCollectorTest
{
    private final BigEndianByteArrayBuffer key = new BigEndianByteArrayBuffer( new byte[4] );
    private final BigEndianByteArrayBuffer value = new BigEndianByteArrayBuffer( new byte[4] );

    @Test
    public void shouldComputePageCatalogue() throws Exception
    {
        // given
        StubCollector collector = new StubCollector( 4 );
        collector.visit( key, value );
        collector.visit( key, value );

        // when
        key.putInt( 0, 24 );
        collector.visit( key, value );
        key.putInt( 0, 62 );
        collector.visit( key, value );

        key.putInt( 0, 78 );
        collector.visit( key, value );
        key.putInt( 0, 84 );
        collector.visit( key, value );
        key.putInt( 0, 96 );
        collector.visit( key, value );

        // then
        assertArrayEquals( new byte[]{/*first:*/0, 0, 0, 24,
                                      /* last:*/0, 0, 0, 62,
                                      /*first:*/0, 0, 0, 78,
                                      /* last:*/0, 0, 0, 96,
        }, collector.pageCatalogue() );
    }

    @Test
    public void shouldComputePageCatalogueOverThreePages() throws Exception
    {
        // given
        StubCollector collector = new StubCollector( 4 );
        collector.visit( key, value );
        collector.visit( key, value );

        // when
        key.putInt( 0, 24 );
        collector.visit( key, value );
        key.putInt( 0, 62 );
        collector.visit( key, value );

        key.putInt( 0, 78 );
        collector.visit( key, value );
        key.putInt( 0, 84 );
        collector.visit( key, value );
        key.putInt( 0, 96 );
        collector.visit( key, value );
        key.putInt( 0, 128 );
        collector.visit( key, value );

        key.putInt( 0, 133 );
        collector.visit( key, value );

        // then
        assertArrayEquals( new byte[]{/*first:*/0, 0, 0, 24,
                                      /* last:*/0, 0, 0, 62,
                                      /*first:*/0, 0, 0, 78,
                                      /* last:*/0, 0, 0, (byte)128,
                                      /*first:*/0, 0, 0, (byte)133,
                                      /* last:*/0, 0, 0, (byte)133,
        }, collector.pageCatalogue() );
    }

    @Test
    public void shouldComputePageCatalogueWhenHeaderCoversEntireFirstPage() throws Exception
    {
        // given
        StubCollector collector = new StubCollector( 4, "a", "b", "c" );
        value.putInt( 0, -1 );
        collector.visit( key, value );
        collector.visit( key, value );
        collector.visit( key, value );
        collector.visit( key, value );

        // when
        key.putInt( 0, 16 );
        collector.visit( key, value );
        key.putInt( 0, 32 );
        collector.visit( key, value );

        // then
        assertArrayEquals( new byte[]{/*first:*/0, 0, 0, 16,
                                      /* last:*/0, 0, 0, 32,
        }, collector.pageCatalogue() );
    }

    @Test
    public void shouldComputePageCatalogueWhenHeaderExceedsFirstPage() throws Exception
    {
        // given
        StubCollector collector = new StubCollector( 4, "a", "b", "c", "d" );
        value.putInt( 0, -1 );
        collector.visit( key, value );
        collector.visit( key, value );
        collector.visit( key, value );
        collector.visit( key, value );
        collector.visit( key, value );

        // when
        key.putInt( 0, 16 );
        collector.visit( key, value );
        key.putInt( 0, 32 );
        collector.visit( key, value );

        // then
        assertArrayEquals( new byte[]{/*first:*/0, 0, 0, 16,
                                      /* last:*/0, 0, 0, 32,
        }, collector.pageCatalogue() );
    }

    @Test
    public void shouldComputeCatalogueWhenSingleDataEntryInPage() throws Exception
    {
        // given
        StubCollector collector = new StubCollector( 4, "a", "b" );
        value.putInt( 0, -1 );
        collector.visit( key, value );
        collector.visit( key, value );
        collector.visit( key, value );

        // when
        key.putInt( 0, 16 );
        collector.visit( key, value );
        key.putInt( 0, 32 );
        collector.visit( key, value );

        // then
        assertArrayEquals( new byte[]{/*first:*/0, 0, 0, 16,
                                      /* last:*/0, 0, 0, 16,
                                      /*first:*/0, 0, 0, 32,
                                      /* last:*/0, 0, 0, 32,
        }, collector.pageCatalogue() );
    }
}