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
package org.neo4j.collection.primitive;

import java.util.Arrays;
import java.util.function.LongPredicate;

import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceUtils;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.resourceIterator;

public class PrimitiveLongResourceCollections
{
    private static final PrimitiveLongResourceIterator EMPTY = new PrimitiveLongBaseResourceIterator( null )
    {
        @Override
        protected boolean fetchNext()
        {
            return false;
        }
    };

    public static PrimitiveLongResourceIterator emptyIterator()
    {
        return EMPTY;
    }

    public static PrimitiveLongResourceIterator iterator( Resource resource, final long... items )
    {
        return resourceIterator( PrimitiveLongCollections.iterator( items ), resource );
    }

    public static PrimitiveLongResourceIterator concat( PrimitiveLongResourceIterator... primitiveLongResourceIterators )
    {
        return concat( Arrays.asList( primitiveLongResourceIterators ) );
    }

    public static PrimitiveLongResourceIterator concat( Iterable<PrimitiveLongResourceIterator> primitiveLongResourceIterators )
    {
        return new PrimitiveLongConcatingResourceIterator( primitiveLongResourceIterators );
    }

    public static PrimitiveLongResourceIterator filter( PrimitiveLongResourceIterator source, LongPredicate filter )
    {
        return new PrimitiveLongFilteringResourceIterator( source )
        {
            @Override
            public boolean test( long item )
            {
                return filter.test( item );
            }
        };
    }

    abstract static class PrimitiveLongBaseResourceIterator extends PrimitiveLongCollections.PrimitiveLongBaseIterator
            implements PrimitiveLongResourceIterator
    {
        private Resource resource;

        PrimitiveLongBaseResourceIterator( Resource resource )
        {
            this.resource = resource;
        }

        @Override
        public void close()
        {
            if ( resource != null )
            {
                resource.close();
                resource = null;
            }
        }
    }

    private static class PrimitiveLongConcatingResourceIterator extends PrimitiveLongCollections.PrimitiveLongConcatingIterator
            implements PrimitiveLongResourceIterator
    {
        private final Iterable<PrimitiveLongResourceIterator> iterators;
        private volatile boolean closed;

        private PrimitiveLongConcatingResourceIterator( Iterable<PrimitiveLongResourceIterator> iterators )
        {
            super( iterators.iterator() );
            this.iterators = iterators;
        }

        @Override
        protected boolean fetchNext()
        {
            return !closed && super.fetchNext();
        }

        @Override
        public void close()
        {
            if ( !closed )
            {
                closed = true;
                ResourceUtils.closeAll( iterators );
            }
        }

    }

    private abstract static class PrimitiveLongFilteringResourceIterator extends PrimitiveLongBaseResourceIterator implements LongPredicate
    {
        private final PrimitiveLongIterator source;

        private PrimitiveLongFilteringResourceIterator( PrimitiveLongResourceIterator source )
        {
            super( source );
            this.source = source;
        }

        @Override
        protected boolean fetchNext()
        {
            while ( source.hasNext() )
            {
                long testItem = source.next();
                if ( test( testItem ) )
                {
                    return next( testItem );
                }
            }
            return false;
        }

        @Override
        public abstract boolean test( long testItem );
    }
}
