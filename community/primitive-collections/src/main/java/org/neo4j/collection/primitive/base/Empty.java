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
package org.neo4j.collection.primitive.base;

import org.neo4j.collection.primitive.PrimitiveCollection;
import org.neo4j.collection.primitive.PrimitiveIntCollection;
import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.collection.primitive.PrimitiveIntVisitor;
import org.neo4j.collection.primitive.PrimitiveLongCollection;
import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectVisitor;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;

public class Empty
{
    public static class EmptyPrimitiveCollection implements PrimitiveCollection
    {
        @Override
        public boolean isEmpty()
        {
            return true;
        }

        @Override
        public void clear()
        {   // Nothing to clear
        }

        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public void close()
        {   // Nothing to close
        }
    }

    public static class EmptyPrimitiveLongCollection extends EmptyPrimitiveCollection
        implements PrimitiveLongCollection
    {
        @Override
        public PrimitiveLongIterator iterator()
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        @Override
        public void visitKeys( PrimitiveLongVisitor visitor )
        {   // No keys to visit
        }
    }

    public static final PrimitiveLongCollection EMPTY_PRIMITIVE_LONG_COLLECTION = new EmptyPrimitiveLongCollection();

    public static class EmptyPrimitiveLongSet extends EmptyPrimitiveLongCollection implements PrimitiveLongSet
    {
        @Override
        public boolean add( long value )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll( PrimitiveLongIterator values )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains( long value )
        {
            return false;
        }

        @Override
        public boolean test( long value )
        {
            return false;
        }

        @Override
        public boolean accept( long value )
        {
            return false;
        }

        @Override
        public boolean remove( long value )
        {
            return false;
        }
    }

    public static final PrimitiveLongSet EMPTY_PRIMITIVE_LONG_SET = new EmptyPrimitiveLongSet();

    public static class EmptyPrimitiveIntCollection extends EmptyPrimitiveCollection
        implements PrimitiveIntCollection
    {
        @Override
        public PrimitiveIntIterator iterator()
        {
            return PrimitiveIntCollections.emptyIterator();
        }

        @Override
        public void visitKeys( PrimitiveIntVisitor visitor )
        {   // No keys to visit
        }
    }

    public static class EmptyPrimitiveIntSet extends EmptyPrimitiveIntCollection implements PrimitiveIntSet
    {
        @Override
        public boolean add( int value )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean addAll( PrimitiveIntIterator values )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean contains( int value )
        {
            return false;
        }

        @Override
        public boolean test( int value )
        {
            return false;
        }

        @Override
        public boolean accept( int value )
        {
            return false;
        }

        @Override
        public boolean remove( int value )
        {
            return false;
        }
    }

    public static final PrimitiveIntSet EMPTY_PRIMITIVE_INT_SET = new EmptyPrimitiveIntSet();

    public static class EmptyPrimitiveLongObjectMap<T> extends EmptyPrimitiveCollection implements PrimitiveLongObjectMap<T>
    {
        @Override
        public T put( long key, T t )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsKey( long key )
        {
            return false;
        }

        @Override
        public T get( long key )
        {
            return null;
        }

        @Override
        public T remove( long key )
        {
            return null;
        }

        @Override
        public <E extends Exception> void visitEntries( PrimitiveLongObjectVisitor<T,E> visitor ) throws E
        {   // No entries to visit
        }

        @Override
        public <E extends Exception> void visitKeys( PrimitiveLongVisitor<E> visitor ) throws E
        {   // No keys to visit
        }

        @Override
        public PrimitiveLongIterator iterator()
        {
            return PrimitiveLongCollections.emptyIterator();
        }
    }

    @SuppressWarnings("unchecked")
    public static final PrimitiveLongObjectMap EMPTY_PRIMITIVE_LONG_OBJECT_MAP = new EmptyPrimitiveLongObjectMap<>();
}
