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
package org.neo4j.kernel.impl.store;

import java.util.function.Predicate;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;

/**
 * Scans all used records in a store, returned {@link ResourceIterable} must be properly used such that
 * its {@link ResourceIterable#iterator() resource iterators} are {@link ResourceIterator#close() closed}
 * after use.
 */
public class Scanner
{
    private Scanner()
    {
    }

    @SafeVarargs
    public static <R extends AbstractBaseRecord> ResourceIterable<R> scan( final RecordStore<R> store,
            final Predicate<? super R>... filters )
    {
        return scan( store, true, filters );
    }

    @SafeVarargs
    public static <R extends AbstractBaseRecord> ResourceIterable<R> scan( final RecordStore<R> store,
            final boolean forward, final Predicate<? super R>... filters )
    {
        return () -> new Scan<>( store, forward, filters );
    }

    private static class Scan<R extends AbstractBaseRecord> extends PrefetchingResourceIterator<R>
    {
        private final PrimitiveLongIterator ids;
        private final RecordCursor<R> cursor;
        private final Predicate<? super R>[] filters;

        Scan( RecordStore<R> store, boolean forward, final Predicate<? super R>... filters )
        {
            this.filters = filters;
            this.ids = new StoreIdIterator( store, forward );
            this.cursor = store.newRecordCursor( store.newRecord() );
            cursor.acquire( 0, RecordLoad.CHECK );
        }

        @Override
        protected R fetchNextOrNull()
        {
            while ( ids.hasNext() )
            {
                if ( cursor.next( ids.next() ) )
                {
                    R record = cursor.get();
                    if ( passesFilters( record ) )
                    {
                        return record;
                    }
                }
            }
            return null;
        }

        private boolean passesFilters( R record )
        {
            for ( Predicate<? super R> filter : filters )
            {
                if ( !filter.test( record ) )
                {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void close()
        {
            cursor.close();
        }
    }
}
