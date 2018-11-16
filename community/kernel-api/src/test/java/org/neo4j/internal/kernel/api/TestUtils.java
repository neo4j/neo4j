/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.internal.kernel.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

final class TestUtils
{
    private TestUtils()
    {
        throw new UnsupportedOperationException( "do not instantiate" );
    }

    @SafeVarargs
    static <T> void assertDistinct( List<T>... lists )
    {
        assertDistinct( Arrays.asList( lists ) );
    }

    static <T> void assertDistinct( List<List<T>> lists )
    {
        Set<T> seen = new HashSet<T>();
        for ( List<T> list : lists )
        {
            for ( T item : list )
            {
                assertTrue( String.format( "%s was seen multiple times", item ), seen.add( item ) );
            }
        }
    }

    @SafeVarargs
    static <T> List<T> concat( List<T>... lists )
    {
        return concat( Arrays.asList( lists ) );
    }

    static <T> List<T> concat( List<List<T>> lists )
    {
        return lists.stream().flatMap( Collection::stream ).collect( Collectors.toList());
    }

    static Callable<List<Long>> singleBatchWorker( Scan<NodeCursor> scan, CursorFactory cursorsFactory, int sizeHint )
    {
        return () -> {
            try ( NodeCursor nodes = cursorsFactory.allocateNodeCursor() )
            {
                List<Long> ids = new ArrayList<>( sizeHint );
                scan.reserveBatch( nodes, sizeHint );
                while ( nodes.next() )
                {
                    ids.add( nodes.nodeReference() );
                }

                return ids;
            }
        };
    }

    static Callable<List<Long>> randomBatchWorker( Scan<NodeCursor> scan, CursorFactory cursorsFactory )
    {
        return () -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            try ( NodeCursor nodes = cursorsFactory.allocateNodeCursor() )
            {
                int sizeHint = random.nextInt( 1, 5 );
                List<Long> ids = new ArrayList<>();
                while ( scan.reserveBatch( nodes, sizeHint ) )
                {
                    while ( nodes.next() )
                    {
                        ids.add( nodes.nodeReference() );
                    }
                }

                return ids;
            }
        };
    }

    static <T> T unsafeGet( Future<T> future )
    {
        try
        {
            return future.get();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    static int count( Cursor cursor )
    {
        int count = 0;
        while ( cursor.next() )
        {
            count++;
        }
        return count;
    }
}
