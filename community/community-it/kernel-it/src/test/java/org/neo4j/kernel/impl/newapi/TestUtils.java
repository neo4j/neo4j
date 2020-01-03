/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.api.list.primitive.LongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;

import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.Scan;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class TestUtils
{
    private TestUtils()
    {
        throw new UnsupportedOperationException( "do not instantiate" );
    }

    static void assertDistinct( LongList... lists )
    {
        assertDistinct( Arrays.asList( lists ) );
    }

    static void assertDistinct( List<LongList> lists )
    {
        MutableLongSet seen = LongSets.mutable.empty();
        for ( LongList list : lists )
        {
            list.forEach( item -> assertTrue( seen.add( item ), format( "%s was seen multiple times", item ) ) );
        }
    }

    static LongList concat( LongList... lists )
    {
        return concat( Arrays.asList( lists ) );
    }

    static LongList concat( List<LongList> lists )
    {
        MutableLongList concat = LongLists.mutable.empty();
        lists.forEach( concat::addAll );
        return concat;
    }

    static <T extends Cursor> Callable<LongList> singleBatchWorker( Scan<T> scan, Supplier<T> supplier, ToLongFunction<T> producer, int sizeHint )
    {
        return () -> {
            try ( T nodes = supplier.get() )
            {
                LongArrayList batch = new LongArrayList();
                scan.reserveBatch( nodes, sizeHint );
                while ( nodes.next() )
                {
                    batch.add( producer.applyAsLong( nodes ) );
                }

                return batch;
            }
        };
    }

    static <T extends Cursor> Callable<LongList> randomBatchWorker( Scan<T> scan, Supplier<T> supplier, ToLongFunction<T> producer )
    {
        return () -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();

            try ( T nodes = supplier.get() )
            {
                int sizeHint = random.nextInt( 1, 5 );
                LongArrayList batch = new LongArrayList();
                while ( scan.reserveBatch( nodes, sizeHint ) )
                {
                    while ( nodes.next() )
                    {
                        batch.add( producer.applyAsLong( nodes ) );
                    }
                }

                return batch;
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

    static File createTemporaryFolder() throws IOException
    {
        File createdFolder = File.createTempFile( "neo4j", "" );
        createdFolder.delete();
        createdFolder.mkdir();
        return createdFolder;
    }
}
