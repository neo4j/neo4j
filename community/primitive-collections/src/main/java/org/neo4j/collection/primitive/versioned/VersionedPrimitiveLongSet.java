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
package org.neo4j.collection.primitive.versioned;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;

public class VersionedPrimitiveLongSet implements VersionedCollection
{
    private final VersionedPrimitiveLongObjectMap<Object> map = new VersionedPrimitiveLongObjectMap<>();
    private final PrimitiveLongSet stableView = new MapToSetAdapter( map.stableView() );
    private final PrimitiveLongSet currentView = new MapToSetAdapter( map.currentView() );

    public PrimitiveLongSet currentView()
    {
        return currentView;
    }

    public PrimitiveLongSet stableView()
    {
        return stableView;
    }

    @Override
    public int currentVersion()
    {
        return map.currentVersion();
    }

    @Override
    public int stableVersion()
    {
        return map.stableVersion();
    }

    @Override
    public void markStable()
    {
        map.markStable();
    }

    private static class MapToSetAdapter implements PrimitiveLongSet
    {
        private final PrimitiveLongObjectMap<Object> delegate;

        private MapToSetAdapter( PrimitiveLongObjectMap<Object> delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public boolean add( long value )
        {
            return delegate.put( value, Boolean.TRUE ) == null;
        }

        @Override
        public boolean addAll( PrimitiveLongIterator values )
        {
            boolean changed = false;
            while ( values.hasNext() )
            {
                changed |= add( values.next() );
            }
            return changed;
        }

        @Override
        public boolean contains( long value )
        {
            return delegate.containsKey( value );
        }

        @Override
        public boolean remove( long value )
        {
            return delegate.remove( value ) != null;
        }

        @Override
        public boolean test( long value )
        {
            return contains( value );
        }

        @Override
        public <E extends Exception> void visitKeys( PrimitiveLongVisitor<E> visitor ) throws E
        {
            delegate.visitKeys( visitor );
        }

        @Override
        public boolean isEmpty()
        {
            return delegate.isEmpty();
        }

        @Override
        public void clear()
        {
            delegate.clear();
        }

        @Override
        public int size()
        {
            return delegate.size();
        }

        @Override
        public void close()
        {
            delegate.close();
        }

        @Override
        public PrimitiveLongIterator iterator()
        {
            return delegate.iterator();
        }
    }
}
